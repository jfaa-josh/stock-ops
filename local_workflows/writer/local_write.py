import os, time, threading, ast, re, sys
from pathlib import Path
from typing import Tuple, Dict, Any
import logging

def import_locals(): # Do this here so that I can first set env vars
    from stockops.data.database import writer as writer_mod
    from stockops.data.database.write_buffer import emit
    return writer_mod, emit


# Logging setup
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
logger = logging.getLogger(__name__)

os.environ["TEST_WRITER"] = "1"

# Same inputs as docker compose
os.environ["BUFFER_STREAM"] = "buf:ingest"
os.environ["BUFFER_BATCH"] = "500"
os.environ["BUFFER_BLOCK_MS"] = "10000"
os.environ["BUFFER_TRIM_MAXLEN"] = "100000"

writer_mod, emit = import_locals()

# Start the writer in background
t = threading.Thread(target=writer_mod.main, daemon=True)
t.start()
time.sleep(3)  # let it create the group

def parse_payload(s: str) -> Tuple[str, str, Dict[str, Any]]:
    # Replace WindowsPath('...') / PosixPath("...") with just '...'
    s2 = re.sub(r"(?:WindowsPath|PosixPath)\((['\"])(.*?)\1\)", r"'\2'", s)
    d = ast.literal_eval(s2)  # safe: only literals after substitution
    return d["db_path"], d["table"], d["row"]

# Feed test payloads
test_data = Path(__file__).resolve().parents[0]/'test_data.txt'
with test_data.open("r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue

        path_str, table, row = parse_payload(line)

        payload = {
            "db_path": Path(path_str),
            "table": table,
            "row": row,
        }

        emit(payload)

t.join() # In reality this would be a never ending service, but for testing I
    # shutdown once test_data.txt is finished
