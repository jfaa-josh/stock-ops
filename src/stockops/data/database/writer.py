import json
import logging
import os
import signal
import sys
import threading
import time
from collections import defaultdict
from collections.abc import ItemsView
from pathlib import Path
from typing import Any

from stockops.config import config
from stockops.data.database import write_buffer as wb
from stockops.data.database.sql_db import SQLiteWriter
from stockops.data.utils import parse_db_filename

# Logging setup
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
logger = logging.getLogger(__name__)

GROUP: str = os.getenv("BUFFER_GROUP", "buf-workers")
CONSUMER_ID: str = os.getenv("BUFFER_CONSUMER", "writer-1")

# batching/loop knobs
COUNT = int(os.getenv("BUFFER_BATCH", "500"))  # max number of messages to read from the Redis stream in one batch
BLOCK_MS = int(
    os.getenv("BUFFER_BLOCK_MS", "10000")
)  # how long (in ms) to block/wait if no new messages arrive before returning
TRIM_MAXLEN = int(
    os.getenv("BUFFER_TRIM_MAXLEN", "100000")
)  # cap the stream length so it doesn’t grow forever (approximate bound)

RECOVER_EVERY_SEC = config.RECOVER_EVERY_SEC
CLAIM_MIN_IDLE_SEC = config.CLAIM_MIN_IDLE_SEC
TRIM_EVERY_SEC = config.TRIM_EVERY_SEC

stop_event = None  # module global

# --- Singletons for TEST_MODE ---
TEST_MODE = os.getenv("TEST_WRITER", "0") == "1"
_FAKE_SERVER = None
_STREAM = None


def request_stop() -> None:
    global stop_event
    if stop_event is None:
        stop_event = threading.Event()
    stop_event.set()


def init_stream():
    """
    Create one shared stream (Redis or FakeRedis) and, in TEST_MODE,
    bind it into write_buffer so producers in the same process share it.
    In production Docker, each process/container will create its own Redis client.
    """
    global _FAKE_SERVER, _STREAM

    if _STREAM is not None:
        return _STREAM  # idempotent

    stream_name = os.getenv("BUFFER_STREAM", "buf:ingest")

    if TEST_MODE:
        import fakeredis

        if _FAKE_SERVER is None:
            _FAKE_SERVER = fakeredis.FakeServer()
        _STREAM = wb.FakeRedisStream(stream_name, server=_FAKE_SERVER)
        wb.bind_stream(_STREAM)
    else:
        redis_url = os.getenv("REDIS_URL")
        if not redis_url:
            raise RuntimeError("REDIS_URL is not set; cannot connect to Redis in production.")
        _STREAM = wb.RedisStream(stream_name, redis_url=redis_url)

    logger.info("Initialized stream '%s' (TEST_MODE=%s)", stream_name, TEST_MODE)
    return _STREAM


def _install_signal_handlers() -> None:
    # Only the main thread of the main interpreter may call signal.signal(...)
    if threading.current_thread() is not threading.main_thread():
        return

    def _handler(signum, frame):
        logger.info("Received signal %s, requesting shutdown...", signum)
        if stop_event is None:
            raise RuntimeError("Signal handler not initialized correctly")
        stop_event.set()

    signal.signal(signal.SIGINT, _handler)
    if hasattr(signal, "SIGTERM"):  # keep Windows happy
        signal.signal(signal.SIGTERM, _handler)


def _recover_pending(
    stream, writer: SQLiteWriter, group: str, consumer: str, count: int = 100, min_idle_s: int = CLAIM_MIN_IDLE_SEC
) -> None:
    """
    Use XAUTOCLAIM to grab stale pending messages and handle them as a normal batch.
    """
    try:
        start = "0-0"  # start is '0-0' initially; then use returned 'next_start' to continue
        while True:
            resp = stream.r.xautoclaim(stream.stream, group, consumer, int(min_idle_s), str(start), count=int(count))
            claimed = []
            next_start = None
            if isinstance(resp, list | tuple):
                if len(resp) == 2:
                    next_start, claimed = resp  # redis-py >= 4/5 common shape: (next_start, messages)
                elif len(resp) == 3:
                    _, claimed, next_start = resp  # legacy/multi-key shape: (key, messages, next_start)
                else:
                    logger.error("Unexpected XAUTOCLAIM response shape: %r", resp)
                    break
            else:
                logger.error("Unexpected XAUTOCLAIM response type: %r", type(resp))
                break

            if not claimed:
                break

            batch = []
            for msg_id, kv in claimed:
                s = kv["json"]
                batch.append((msg_id, json.loads(s)))

            ok_ids = _batch_to_writer(writer, batch)
            if ok_ids:
                stream.ack(group, ok_ids)
                stream.delete(ok_ids)

            if not next_start or next_start == "0-0":
                break

            start = str(next_start)
    except Exception:
        logger.exception("Error during pending recovery with XAUTOCLAIM")


def _group_by_file_table(
    msgs: list[tuple[str, dict[str, str]]],
) -> ItemsView[tuple[str, str], list[tuple[str, dict[str, Any]]]]:
    logger.debug("Sorting %s messages recieved in match", len(msgs))

    groups = defaultdict(list)
    for mid, payload in msgs:
        groups[(payload["db_path"], payload["table"])].append((mid, payload))

    return groups.items()


def _batch_to_writer(writer: SQLiteWriter, msgs: list[tuple[str, dict[str, Any]]]) -> list[str]:
    """
    Write to SQLite and return list of msg IDs that were successfully processed.
    Each message payload is expected to look like:
      {"db_path": "...", "table": "...", "row": {...}}
    """
    if not msgs:
        return []

    grouped_msgs = _group_by_file_table(msgs)

    ok_all: list[str] = []
    for (db_path_str, table), batch in grouped_msgs:
        db_path = Path(db_path_str)
        file_str = db_path.name
        parsed_dict = parse_db_filename(file_str)
        ok_ids = writer.insert_many(
            db_path, table, batch, parsed_dict["provider"], parsed_dict["exchange"], parsed_dict["data_type"]
        )
        ok_all.extend(ok_ids)
    return ok_all


def _log_buffer_stats(stream, group: str) -> None:
    """Best-effort, low-noise snapshot of buffer health."""
    try:
        r = getattr(stream, "r", None)
        name = getattr(stream, "stream", None)
        if r is None or name is None:
            logger.debug("Buffer stats: no Redis client attached.")
            return

        # XLEN (total entries)
        try:
            length = r.xlen(name)
        except Exception:
            length = None

        # XPENDING summary (pending count)
        pending_count = None
        try:
            xp = r.xpending(name, group)
            # redis-py >=4: dict {'pending': int, 'min': id, 'max': id, 'consumers': [...]}
            if isinstance(xp, dict):
                pending_count = xp.get("pending")
            elif isinstance(xp, list | tuple) and xp:
                # older return style: (count, smallest, greatest, consumers)
                pending_count = xp[0]
        except Exception:
            pass

        # Oldest idle ms (one-item range)
        oldest_idle_ms = None
        try:
            # redis-py >=4
            rng = r.xpending_range(name, group, min="-", max="+", count=1)
            if rng:
                e0 = rng[0]
                oldest_idle_ms = e0.get("idle") if isinstance(e0, dict) else getattr(e0, "idle", None)
        except Exception:
            # legacy fallback: XPENDING <stream> <group> - + 1
            try:
                rng = r.xpending(name, group, "-", "+", 1)
                if rng:
                    e0 = rng[0]
                    if isinstance(e0, dict):
                        oldest_idle_ms = e0.get("idle")
                    elif isinstance(e0, list | tuple) and len(e0) >= 4:
                        # (id, consumer, idle, deliveries)
                        oldest_idle_ms = e0[2]
            except Exception:
                pass

        # XINFO GROUPS (consumers, last-delivered-id)
        consumers = last_delivered = None
        try:
            for g in r.xinfo_groups(name):
                gname = g.get("name")
                if gname == group or (isinstance(gname, bytes) and gname.decode() == group):
                    consumers = g.get("consumers")
                    last_delivered = g.get("last-delivered-id")
                    break
        except Exception:
            pass

        logger.info(
            "buffer stats | stream=%s len=%s pending=%s oldest_idle_ms=%s consumers=%s last_delivered=%s",
            name,
            length,
            pending_count,
            oldest_idle_ms,
            consumers,
            last_delivered,
        )
    except Exception:
        logger.exception("Failed to log buffer stats")


def main() -> None:
    global stop_event
    stop_event = threading.Event()
    _install_signal_handlers()

    if _STREAM is None:
        init_stream()

    stream = _STREAM
    if stream is None:
        raise RuntimeError("Stream is not initialized properly")

    stream.ensure_group(GROUP)
    writer = SQLiteWriter()

    last_trim = 0.0
    last_recover = 0.0

    try:
        logger.info(
            "Buffer starting with stream=%s group=%s consumer=%s",
            os.getenv("BUFFER_STREAM", "buf:ingest"),
            GROUP,
            CONSUMER_ID,
        )
        _log_buffer_stats(stream, GROUP)

        # ---- tighten block time so stop_event is honored promptly ----
        block_once_ms = max(1, min(BLOCK_MS, 1000))

        while not stop_event.is_set():
            batch = stream.read_group(GROUP, CONSUMER_ID, count=COUNT, block_ms=block_once_ms)
            if not batch:
                now = time.time()
                if TRIM_MAXLEN and now - last_trim > TRIM_EVERY_SEC:
                    logger.debug("No messages; attempting trim_maxlen...")
                    stream.trim_maxlen(TRIM_MAXLEN)
                    last_trim = now
                    logger.debug("Stream trimmed to ~%d entries", TRIM_MAXLEN)
                continue

            if batch:
                logger.debug("Fetched batch of %d message(s) from group=%s consumer=%s", len(batch), GROUP, CONSUMER_ID)

            try:
                ok_ids = _batch_to_writer(writer, batch)
                if ok_ids:
                    stream.ack(GROUP, ok_ids)
                    stream.delete(ok_ids)  # Hard-delete to keep the stream tiny when reads succeed
                logger.debug("Inserted %d row(s) into SQLite; acking+deleting.", len(ok_ids))
            except Exception:
                # Do not ack on failure; entries remain pending for retry or manual claim; Log and keep going
                logger.exception("Writer error while handling %d message(s); leaving them pending.", len(batch))

            now = time.time()
            if now - last_recover > RECOVER_EVERY_SEC:
                _log_buffer_stats(stream, GROUP)
                # pass seconds or ms; function normalizes
                _recover_pending(stream, writer, GROUP, CONSUMER_ID)
                last_recover = now

    finally:
        _log_buffer_stats(stream, GROUP)
        logger.info("Shutting down writer: flushing & closing SQLite and stream.")
        try:
            # Drain anything left (claim regardless of idle time)
            _recover_pending(stream, writer, GROUP, CONSUMER_ID, count=max(1000, COUNT), min_idle_s=0)
        except Exception:
            logger.exception("Error during final pending recovery")

        # Always close the DB first
        try:
            writer.close()
        except Exception:
            logger.exception("Error while closing SQLiteWriter")

        # Close redis client to kill any helper threads
        try:
            r = getattr(stream, "r", None)
            if r is not None:
                try:
                    r.close()  # redis-py 4+
                except Exception:
                    pass
                try:
                    # Ensures all sockets are closed and background helpers stop
                    pool = getattr(r, "connection_pool", None)
                    if pool is not None:
                        pool.disconnect()
                except Exception:
                    pass
        except Exception:
            logger.exception("Error while closing Redis client")

        logger.info("Shutdown complete.")


if __name__ == "__main__":
    main()
