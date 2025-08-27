from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Protocol, cast, runtime_checkable

logger = logging.getLogger(__name__)


@runtime_checkable
class _BaseStream(Protocol):
    def emit(self, payload: dict[str, Any]) -> str: ...
    def ensure_group(self, group: str) -> None: ...
    def read_group(self, group: str, consumer: str, count: int, block_ms: int) -> list[tuple[str, dict[str, Any]]]: ...
    def ack(self, group: str, ids: list[str]) -> int: ...
    def delete(self, ids: list[str]) -> int: ...
    def trim_maxlen(self, maxlen: int) -> None: ...


class RedisStream(_BaseStream):
    """
    Real Redis (docker). Uses streams so writer can XREADGROUP and ack.
    REDIS_URL like: redis://redis:6379/1
    """

    def __init__(self, stream: str, redis_url: str):
        import redis

        self.stream = stream
        self.r = redis.from_url(redis_url, decode_responses=True)

    def emit(self, payload: dict[str, Any]) -> str:
        return cast(str, self.r.xadd(self.stream, {"json": json.dumps(payload, separators=(",", ":"))}))

    def ensure_group(self, group: str) -> None:
        try:
            self.r.xgroup_create(self.stream, group, id="0-0", mkstream=True)
        except Exception as e:
            # Group already exists => ignore
            if "BUSYGROUP" not in str(e):
                raise

    def read_group(self, group: str, consumer: str, count: int, block_ms: int) -> list[tuple[str, dict[str, Any]]]:
        """
        Try to accumulate up to `count` messages without exceeding `block_ms` total wall time.
        Returns as soon as we hit `count` OR the deadline passes.
        """
        deadline = time.monotonic() + (block_ms / 1000.0)
        out: list[tuple[str, dict[str, Any]]] = []
        first = True
        TOPUP_MS = 25  # small coalescing window per top-up read

        while len(out) < count:
            remaining_ms = max(0, int((deadline - time.monotonic()) * 1000))
            if remaining_ms == 0 and not first:
                break

            per_call_block = remaining_ms if first else min(remaining_ms, TOPUP_MS)
            want = count - len(out)

            resp = cast(
                list[tuple[str, list[tuple[str, dict[str, Any]]]]],
                self.r.xreadgroup(
                    groupname=group,
                    consumername=consumer,
                    streams={self.stream: ">"},
                    count=want,
                    block=per_call_block if per_call_block > 0 else 0,
                ),
            )
            if not resp:
                # nothing arrived during this (top-up) window; loop until deadline
                first = False
                continue

            _, entries = resp[0]
            for msg_id, kv in entries:
                s = kv["json"]
                out.append((msg_id, json.loads(s)))

            first = False

        return out

    def ack(self, group: str, ids: list[str]) -> int:
        if not ids:
            return 0
        return cast(int, self.r.xack(self.stream, group, *ids))

    def delete(self, ids: list[str]) -> int:
        if not ids:
            return 0
        return cast(int, self.r.xdel(self.stream, *ids))

    def trim_maxlen(self, maxlen: int) -> None:
        self.r.xtrim(self.stream, maxlen=maxlen, approximate=True)


class FakeRedisStream(_BaseStream):
    """
    Local (single process) using fakeredis—same API as RedisStream.
    Useful when you want to run everything in one Python process.
    """

    def __init__(self, stream: str, server=None):
        import fakeredis

        self.stream = stream
        self.r = fakeredis.FakeRedis(server=server, decode_responses=True)

    def emit(self, payload: dict[str, Any]) -> str:
        return cast(str, self.r.xadd(self.stream, {"json": json.dumps(payload, separators=(",", ":"))}))

    def ensure_group(self, group: str) -> None:
        try:
            self.r.xgroup_create(self.stream, group, id="0-0", mkstream=True)
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                raise

    def read_group(self, group: str, consumer: str, count: int, block_ms: int) -> list[tuple[str, dict[str, Any]]]:
        """
        Try to accumulate up to `count` messages without exceeding `block_ms` total wall time.
        Returns as soon as we hit `count` OR the deadline passes.
        """
        deadline = time.monotonic() + (block_ms / 1000.0)
        out: list[tuple[str, dict[str, Any]]] = []
        first = True
        TOPUP_MS = 25  # small coalescing window per top-up read

        while len(out) < count:
            remaining_ms = max(0, int((deadline - time.monotonic()) * 1000))
            if remaining_ms == 0 and not first:
                break

            per_call_block = remaining_ms if first else min(remaining_ms, TOPUP_MS)
            want = count - len(out)

            resp = cast(
                list[tuple[str, list[tuple[str, dict[str, Any]]]]],
                self.r.xreadgroup(
                    groupname=group,
                    consumername=consumer,
                    streams={self.stream: ">"},
                    count=want,
                    block=per_call_block if per_call_block > 0 else 0,
                ),
            )
            if not resp:
                # nothing arrived during this (top-up) window; loop until deadline
                first = False
                continue

            _, entries = resp[0]
            for msg_id, kv in entries:
                s = kv["json"]
                out.append((msg_id, json.loads(s)))

            first = False

        return out

    def ack(self, group: str, ids: list[str]) -> int:
        return cast(int, self.r.xack(self.stream, group, *ids) if ids else 0)

    def delete(self, ids: list[str]) -> int:
        return cast(int, self.r.xdel(self.stream, *ids) if ids else 0)

    def trim_maxlen(self, maxlen: int) -> None:
        self.r.xtrim(self.stream, maxlen=maxlen, approximate=True)


# ---------- Binding from writer ----------
_STREAM_SINGLETON: _BaseStream | None = None
_FAKE_MODE_BOUND: bool = False  # track if TEST_MODE stream was bound


def bind_stream(stream: _BaseStream) -> None:
    """Used in TEST_MODE (single-process) to inject a shared FakeRedis stream."""
    global _STREAM_SINGLETON, _FAKE_MODE_BOUND
    _STREAM_SINGLETON = stream
    _FAKE_MODE_BOUND = True


def _get_stream_for_emit() -> _BaseStream:
    """
    Production: lazily construct a RedisStream using env if not already set.
    Test mode: require an explicitly bound FakeRedis stream (shared FakeServer).
    """
    global _STREAM_SINGLETON
    if _STREAM_SINGLETON is not None:
        return _STREAM_SINGLETON  # If producer already instanced a stream, re-use it.

    TEST_WRITER = os.getenv("TEST_WRITER", "0") == "1"
    stream_name = os.getenv("BUFFER_STREAM", "buf:ingest")

    if TEST_WRITER:
        # In TEST_WRITER we *require* binding so producers and consumer share the same FakeServer.
        if not _FAKE_MODE_BOUND or _STREAM_SINGLETON is None:
            raise RuntimeError(
                "TEST_WRITER=1 but no stream bound. Call writer.init_stream()/bind_stream() before emit()."
            )
        return _STREAM_SINGLETON  # writer.init_stream() controls this stream in the test case

    # ---- PRODUCTION PATH (Docker, real Redis) ----
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        # Fail fast—inside containers “localhost” won’t reach the Redis service.
        raise RuntimeError("REDIS_URL is not set; cannot connect to Redis in production.")

    _STREAM_SINGLETON = RedisStream(stream_name, redis_url=redis_url)
    return _STREAM_SINGLETON


# # --------- Producer-facing, single-call API ---------
# # Producers call ONLY this function:
def emit(payload: dict[str, Any]) -> str:
    try:  # Converting rcvd pathobject to str for json serializing, reconvd b4 .db writing
        path_obj = payload["db_path"]
        payload["db_path"] = str(path_obj)
        stream = _get_stream_for_emit()
        return stream.emit(payload)
    except Exception as e:
        raise e
