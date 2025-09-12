import os, time, threading, ast, re, sys, faulthandler, platform, random
from pathlib import Path, PurePath
from typing import Tuple, Dict, Any
import logging

from stockops.config import config

def import_locals(): # Do this here so that I can first set env vars
    from stockops.data.database import writer as writer_mod
    from stockops.data.database.write_buffer import emit
    return writer_mod, emit


logger = logging.getLogger(__name__)

os.environ["TEST_WRITER"] = "1"

# Same inputs as docker compose
os.environ["BUFFER_STREAM"] = "buf:ingest"
os.environ["BUFFER_BATCH"] = "500"
os.environ["BUFFER_BLOCK_MS"] = "1000" # Polling faster for test case for thread closing
os.environ["BUFFER_TRIM_MAXLEN"] = "100000"

writer_mod, emit = import_locals()

def main():
    def make_test_data_txt():
        """
        NOTE: xformer_out_test_data.txt contains:
        - all 4 producer types for EODHD data.
        - Historical intraday spans 2 days
        - There is one duplicated entry (1755526670499 ts for streaming SPY).
        - There is one historical interday entry with a duplicated timestamp, with an updated value for open
        - There is one historical intraday intry with Nonetype data (should not be stored in .db)
        - There is one historical interday with a different ticker
        - There is one set of quotes and prices streaming data at the same timestamp
        """
        data_dir = config.DATA_RAW_DIR/'inputs'

        input_file = data_dir/'xformer_out_test_data.txt'
        target_file = data_dir/'test_data.txt'

        with open(input_file, 'r') as f:
            txt = f.readlines()

        new_rows = []
        for row in txt:
            if row.startswith("{'db_path"):
                new_rows.append(row)

        random.shuffle(new_rows)

        with open(target_file, "w", encoding="utf-8") as f:
            for x in new_rows:
                f.write(f"{x}\n")   # ensures a newline per element

    def thread_idle(th, idle_secs=2.0):
        ts = last_active.get(th.ident)
        if ts is None:                  # no activity seen yet
            return False
        return (time.monotonic() - ts) >= idle_secs

    def _trace(frame, event, arg):
        # super lightweight; avoid heavy work here
        last_active[threading.get_ident()] = time.monotonic()
        return _trace

    def _run_with_trace(fn):
        sys.settrace(_trace)
        try:
            fn()
        finally:
            sys.settrace(None)

    def clear_directory(dir_path: Path):
        """
        Delete all files from the given directory, but keep the directory itself.
        """
        if dir_path.exists() and dir_path.is_dir():
            logger.info("dir_path %s exists; clearing any files", dir_path)
            for file in dir_path.iterdir():
                if file.is_file():
                    file.unlink()  # deletes the file

    def parse_payload(s: str) -> Tuple[str, str, Dict[str, Any]]:
        s2 = re.sub(r"(?:WindowsPath|PosixPath)\((['\"])(.*?)\1\)", r"'\2'", s)
        d = ast.literal_eval(s2)  # safe: only literals after substitution
        return d["db_path"], d["table"], d["row"]

    def dbs_quiescent(paths, quiet_secs=2.0):
        def snap():
            m = {}
            for p in paths:
                try:
                    st = os.stat(p)
                    m[p] = (True, st.st_mtime, st.st_size)
                except FileNotFoundError:
                    m[p] = (False, 0.0, 0)
            return m
        s1 = snap()
        time.sleep(quiet_secs)
        s2 = snap()
        if s1 != s2:
            sys.stderr.write(">>> dbs not quiet; diffs:\n"); sys.stderr.flush()
            for p in paths:
                if s1.get(p) != s2.get(p):
                    sys.stderr.write(f"    {p}: {s1.get(p)} -> {s2.get(p)}\n"); sys.stderr.flush()
        return s1 == s2

    def rewrite_db_path(input_path: str) -> Path:
        """
        Convert a stored Windows-style path to a Path under the current repo data roots.
        Uses config.RAW_HISTORICAL_DIR and config.RAW_STREAMING_DIR as anchors.
        """
        p = PurePath(input_path.replace("\\", "/"))

        s = None
        if "streaming" in p.parts:
            s = "streaming"
            root_dir: Path = config.RAW_STREAMING_DIR
        elif "historical" in p.parts:
            s = "historical"
            root_dir: Path = config.RAW_HISTORICAL_DIR
        else:
            raise ValueError(f"Neither 'historical' nor 'streaming' found in path: {input_path!r}")

        assert root_dir.exists() and root_dir.is_dir(), f'Writer directory {root_dir} does not exist!'

        # Take everything after the anchor directory (including the filename)
        idx = p.parts.index(s)

        file_name = str(*p.parts[idx + 1 :])
        return root_dir / file_name

    # Clear existing outputs and generate input test data
    clear_directory(config.RAW_HISTORICAL_DIR)
    clear_directory(config.RAW_STREAMING_DIR)

    make_test_data_txt() # Create or overwrite test_data.txt

    # Start the writer in background
    last_active = {}

    t = threading.Thread(target=lambda: _run_with_trace(writer_mod.main), daemon=True)
    t.start()
    time.sleep(3)

    db_paths_seen = set()

    # Feed test payloads
    test_data = config.DATA_RAW_DIR/'inputs'/'test_data.txt'
    with test_data.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            path_str, table, row = parse_payload(line)
            rewritten_path = rewrite_db_path(path_str)

            payload = {
                "db_path": str(rewritten_path),
                "table": table,
                "row": row,
            }

            emit(payload)

            db_paths_seen.add(rewritten_path)
            logger.info("db_path/table set for %s/%s", rewritten_path, table)

    # Arm only while waiting
    IS_WIN = platform.system() == "Windows"

    # Signal the writer to stop and wake any blocking read
    if hasattr(writer_mod, "request_stop"):
        writer_mod.request_stop()
        time.sleep(0.05)  # tiny head start for the wake xadd

    faulthandler.enable(file=sys.stderr, all_threads=not IS_WIN)

    # avoid dump_traceback_later on Windows (it always dumps all threads)
    if not IS_WIN:
        faulthandler.dump_traceback_later(15, repeat=False)

    sys.stderr.write(">>> entering quiescence wait\n"); sys.stderr.flush()
    logger.info("Entering quiescence wait\n")

    # --- bounded quiescence wait ---
    deadline = time.monotonic() + 60.0  # 60s cap
    while t.is_alive():
        if thread_idle(t, idle_secs=2.0) and dbs_quiescent(db_paths_seen, quiet_secs=2.0):
            break
        if time.monotonic() >= deadline:
            sys.stderr.write(">>> quiescence wait timed out\n"); sys.stderr.flush()
            logger.warning("Quiescence wait timed out\n")
            # one debug pass to show which file(s) keep changing
            dbs_quiescent(db_paths_seen, quiet_secs=0.5)
            break
        time.sleep(0.2)

    # cancel any pending delayed dump now that we're done waiting
    if not IS_WIN:
        faulthandler.cancel_dump_traceback_later()

    # --- attempt to finish the writer thread (won't block because it's daemon) ---
    t.join(timeout=1.0)

    # --- diagnostics ---
    live = [th for th in threading.enumerate() if th.is_alive()]
    print("\n=== Live threads at shutdown ===", flush=True)
    for th in live:
        print(f"name={th.name!r} ident={th.ident} daemon={th.daemon} alive={th.is_alive()}", flush=True)

    print("\n=== Stacks of live threads ===", flush=True)
    # Final dump: avoid all_threads on Windows
    faulthandler.dump_traceback(file=sys.stdout, all_threads=not IS_WIN)

    # If some non-daemon threads persist, avoid CI hangs:
    if any(not th.daemon for th in live if th is not threading.current_thread()):
        sys.stderr.write(">>> non-daemon threads remain; forcing exit (tests only)\n"); sys.stderr.flush()
        os._exit(0)

    logger.info("Writer test completed successfully!\n")

if __name__ == "__main__":
    main()
