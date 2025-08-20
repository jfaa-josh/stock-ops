import json
import logging
import os
import sqlite3
from pathlib import Path
from typing import Any, TypedDict

from stockops.config import utils as cfg_utils  # , add additional providers here as needed

logger = logging.getLogger(__name__)


class _TableCache(TypedDict):
    cols: list[str]
    insert_cols: list[str]
    insert_sql: str


class SQLiteWriter:
    def __init__(self):
        self._conn: sqlite3.Connection | None = None
        self._cursor: sqlite3.Cursor | None = None
        self._open_for: tuple[Path, str] | None = None
        self._table_cache: dict[tuple[Path, str], _TableCache] = {}

    def parse_provider_exchange(self, db_filepath: Path) -> tuple[str, str]:
        name = db_filepath.stem
        parts = name.split("_")

        if parts[0] == "historical":
            # historical_interday_EODHD_US...
            return parts[2], parts[3]
        elif parts[0] == "streaming":
            # streaming_EODHD_US_...
            return parts[1], parts[2]
        else:
            raise ValueError(f"Unexpected filename format: {name}")

    def _ensure_open(self, db_filepath: Path, table_name: str, data: dict[str, Any]) -> None:
        """Open (db_filepath, table_name) if needed, create/evolve schema, create indexes, seed metadata."""
        target = (db_filepath, table_name)
        needs_open = (self._open_for != target) or (self._conn is None) or (self._cursor is None)

        if not needs_open:
            return

        self.close()  # Close any prior DB

        # --- Path sanity & directory creation ---
        db_filepath = Path(db_filepath)
        parent = db_filepath.parent
        try:
            parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise RuntimeError(f"Failed to create parent directory {parent}: {e}") from e
        if parent.is_file():
            raise RuntimeError(f"DB parent is a file, not a directory: {parent}")
        if db_filepath.exists() and db_filepath.is_dir():
            raise RuntimeError(f"DB path points to a directory: {db_filepath}")

        # Ensure parent is writable *and* traversable
        if not os.access(parent, os.W_OK | os.X_OK):
            st = parent.stat()
            mode = oct(st.st_mode & 0o777)
            owner = getattr(st, "st_uid", "N/A")
            group = getattr(st, "st_gid", "N/A")
            raise RuntimeError(f"DB parent not writable/traversable: {parent} (mode={mode} uid={owner} gid={group})")

        # If DB file missing but sidecars remain from a previous run, remove them
        if not db_filepath.exists():
            for suf in ("-wal", "-shm"):
                sidecar = Path(str(db_filepath) + suf)
                try:
                    if sidecar.exists():
                        sidecar.unlink()
                except Exception:
                    logger.debug("Could not remove orphan sidecar %s", sidecar, exc_info=True)

        existed_before = db_filepath.exists()

        # Open connection (single call) with richer diagnostics on failure
        try:
            conn = sqlite3.connect(str(db_filepath))
        except sqlite3.OperationalError as e:
            try:
                st = parent.stat()
                mode = oct(st.st_mode & 0o777)
                owner = st.st_uid
                group = st.st_gid
            except Exception:
                mode = owner = group = "?"
            raise RuntimeError(
                f"Failed to open SQLite DB at {db_filepath} "
                f"(parent={parent}, exists={parent.exists()}, mode={mode}, uid={owner}, gid={group})"
            ) from e

        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA wal_autocheckpoint=1000")
        conn.execute("PRAGMA busy_timeout=5000;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Seed DB-level metadata once per DB (first open on a new file)
        if not existed_before:
            cur.execute('CREATE TABLE IF NOT EXISTS "__meta__" (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
            try:
                provider, exchange = self.parse_provider_exchange(db_filepath)
                meta = cfg_utils.ProviderConfig(provider, exchange).cfg.EXCHANGE_METADATA[exchange]
            except Exception:
                logger.warning("Skipping provider metadata seeding; parse/lookup failed.", exc_info=True)
                meta = {}
                provider, exchange = "UNKNOWN", "UNKNOWN"
            rows = [(str(k), json.dumps(v) if isinstance(v, dict) else str(v)) for k, v in meta.items()]
            rows.extend([("provider", provider), ("exchange", exchange), ("created_utc", "CURRENT_TIMESTAMP")])
            cur.executemany('INSERT OR REPLACE INTO "__meta__"(key, value) VALUES (?, ?)', rows)
            # Fix created_utc to real timestamp (replace literal)
            cur.execute('UPDATE "__meta__" SET value = CURRENT_TIMESTAMP WHERE key = "created_utc"')

        # Optional fast stats table
        cur.execute(
            'CREATE TABLE IF NOT EXISTS "__table_stats__" ('
            "table_name TEXT PRIMARY KEY, "
            "row_count INTEGER NOT NULL DEFAULT 0, "
            "min_timestamp_utc_s REAL, "
            "max_timestamp_utc_s REAL, "
            "min_date TEXT, "
            "max_date TEXT, "
            "updated_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
        )

        # --- Create baseline table if missing, then evolve columns ---
        # Ensure 'version' is always present as INTEGER NOT NULL DEFAULT 1
        wanted_cols = list(dict.fromkeys([*data.keys(), "version"]))

        # If table is new, create with all known cols; version is INTEGER
        # New data-derived columns are TEXT (flexible ingestion)
        col_defs = []
        for k in wanted_cols:
            if k == "version":
                col_defs.append('"version" INTEGER NOT NULL DEFAULT 1')
            else:
                col_defs.append(f'"{k}" TEXT')
        cur.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(col_defs)})')

        # Evolve: add any missing cols (version integer; others TEXT, NULL default)
        cur.execute(f'PRAGMA table_info("{table_name}")')
        existing_cols = {row["name"] for row in cur.fetchall()}

        schema_changed = False
        if "version" not in existing_cols:
            cur.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "version" INTEGER NOT NULL DEFAULT 1')
            schema_changed = True

        for col in wanted_cols:
            if col in existing_cols or col == "version":
                continue
            cur.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" TEXT')
            schema_changed = True

        # Create recommended indexes if applicable (idempotent)
        cur.execute(f'PRAGMA table_info("{table_name}")')
        existing_cols = {row["name"] for row in cur.fetchall()}  # refresh if changed

        if {"timestamp_UTC_s", "interval"} <= existing_cols:
            cur.execute(
                f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_ts_interval" '
                f'ON "{table_name}"("timestamp_UTC_s","interval")'
            )
        elif {"timestamp_UTC_ms", "interval"} <= existing_cols:
            cur.execute(
                f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_ts_ms_interval" '
                f'ON "{table_name}"("timestamp_UTC_ms","interval")'
            )
        elif {"date", "interval"} <= existing_cols:
            cur.execute(
                f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_date_interval" ON "{table_name}"("date","interval")'
            )
        # If you supply a unique message id in incoming rows, index it:
        if "msg_id" in existing_cols:
            cur.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS "idx_{table_name}_msg_id" ON "{table_name}"("msg_id")')

        conn.commit()
        self._conn, self._cursor = conn, cur
        self._open_for = target

        # Invalidate cached prepared SQL if schema changed
        if schema_changed:
            self._table_cache.pop(target, None)

    def insert_many(self, db_filepath: Path, table_name: str, batch: list[tuple[str, dict[str, Any]]]) -> list[str]:
        """
        Insert rows for (db_filepath, table_name) with versioning and dedupe semantics.

        Returns a List[msg_id]: Append msg_id if row was inserted (changed count increased).
        Duplicates ignored by UNIQUE (e.g., msg_id).

        Efficiency features:
        - integer 'version' column
        - cached table metadata & prepared INSERT
        - batched MAX(version) lookups for pair keys
        - single transaction for entire batch (no per-row savepoints)
        """
        if not batch:
            return []

        msg_ids: list[str] = []
        rows: list[dict[str, Any]] = []
        for _id, payload in batch:
            msg_ids.append(_id)
            rows.append(dict(payload["row"]))

        # 1) Union of batch keys (exclude 'version' – managed internally)
        all_keys: list[str] = []
        seen = set()
        for r in rows:
            for k in r.keys():
                if k == "version":
                    continue
                if k not in seen:
                    seen.add(k)
                    all_keys.append(k)

        # Ensure 'msg_id' and 'version' exists in schema
        ensure_keys = [*all_keys, "msg_id", "version"]
        self._ensure_open(db_filepath, table_name, dict.fromkeys(ensure_keys))
        if self._conn is None or self._cursor is None:
            raise RuntimeError("Database connection not open")

        conn, cur = self._conn, self._cursor
        cache_key = (db_filepath, table_name)

        # 2) Load or build cached metadata & prepared INSERT
        tbl_cache: _TableCache | None = self._table_cache.get(cache_key)
        if tbl_cache is None:
            cur.execute(f'PRAGMA table_info("{table_name}")')
            table_cols: list[str] = [str(row["name"]) for row in cur.fetchall()]

            wanted: set[str] = set(all_keys) | {"msg_id"}
            insert_cols: list[str] = [c for c in table_cols if c in wanted] + (
                ["version"] if "version" in table_cols else []
            )

            placeholders: str = ", ".join("?" for _ in insert_cols)
            columns_sql: str = ", ".join(f'"{c}"' for c in insert_cols)
            insert_sql: str = f'INSERT OR IGNORE INTO "{table_name}" ({columns_sql}) VALUES ({placeholders})'

            # Build a value that is statically _TableCache
            new_cache: _TableCache = {
                "cols": table_cols,
                "insert_cols": insert_cols,
                "insert_sql": insert_sql,
            }
            self._table_cache[cache_key] = new_cache
            cache: _TableCache = new_cache
        else:
            cache = tbl_cache  # already _TableCache

        table_cols = cache["cols"]
        insert_cols = cache["insert_cols"]
        insert_sql = cache["insert_sql"]

        # dynamic pair-key selection (first match)
        pair_cols: tuple[str, str] | None = None
        if {"timestamp_UTC_s", "interval"} <= set(table_cols):
            pair_cols = ("timestamp_UTC_s", "interval")
        elif {"timestamp_UTC_ms", "interval"} <= set(table_cols):
            pair_cols = ("timestamp_UTC_ms", "interval")
        elif {"date", "interval"} <= set(table_cols):
            pair_cols = ("date", "interval")

        pair_to_maxv: dict[tuple[Any, Any], int] = {}
        if pair_cols is not None:
            # collect unique pairs present in this batch
            p0, p1 = pair_cols
            pairs: set[tuple[Any, Any]] = set()
            for r in rows:
                if p0 in r and p1 in r:
                    pairs.add((r.get(p0, None), r.get(p1, None)))
            if pairs:
                # Build a single batched query
                # SQLite lacks tuple IN for parameters directly; emulate with OR chain
                where_terms = []
                params: list[Any] = []
                for a, b in pairs:
                    if a is None and b is None:
                        where_terms.append(f'("{p0}" IS NULL AND "{p1}" IS NULL)')
                    elif a is None:
                        where_terms.append(f'("{p0}" IS NULL AND "{p1}" = ?)')
                        params.append(b)
                    elif b is None:
                        where_terms.append(f'("{p0}" = ? AND "{p1}" IS NULL)')
                        params.append(a)
                    else:
                        where_terms.append(f'("{p0}" = ? AND "{p1}" = ?)')
                        params.extend([a, b])
                sql = (
                    f'SELECT "{p0}" AS k0, "{p1}" AS k1, MAX("version") AS maxv '
                    f'FROM "{table_name}" '
                    f"WHERE " + " OR ".join(where_terms) + " "
                    f'GROUP BY "{p0}", "{p1}"'
                )
                cur.execute(sql, params)
                for row in cur.fetchall():
                    pair_key = (row["k0"], row["k1"])
                    pair_to_maxv[pair_key] = int(row["maxv"]) if row["maxv"] is not None else 0

        # 4) Insert loop: single transaction, per-row try/except (no savepoints)
        ok_ids: list[str] = []
        inserted_count = 0
        # Precompute for stats update
        batch_min_ts = None  # seconds (if ms provided, convert /1000 when numeric)
        batch_max_ts = None
        batch_min_date = None
        batch_max_date = None

        try:
            conn.execute("BEGIN IMMEDIATE")

            for idx, r in enumerate(rows):
                before = conn.total_changes

                # --- Empty-row filter: skip rows where all data (except time &/| interval) are None
                ts_key = (
                    "timestamp_UTC_s"
                    if "timestamp_UTC_s" in r
                    else ("timestamp_UTC_ms" if "timestamp_UTC_ms" in r else ("date" if "date" in r else None))
                )
                non_data = {"interval"}
                if ts_key:
                    non_data.add(ts_key)
                if all(r.get(k) is None for k in r.keys() if k not in non_data):
                    # Do not store; processed for idempotency/ACK purposes
                    ok_ids.append(msg_ids[idx])
                    continue

                # Determine version
                version_val = 1
                if pair_cols is not None:
                    p0, p1 = pair_cols
                    if p0 in r and p1 in r:
                        key = (r.get(p0, None), r.get(p1, None))
                        maxv = pair_to_maxv.get(key)
                        if maxv is not None and maxv > 0:
                            version_val = maxv + 1

                # Prepare values aligned to insert_cols
                vals: list[Any] = []
                for c in insert_cols:
                    if c == "version":
                        vals.append(version_val)
                    elif c == "msg_id":
                        vals.append(msg_ids[idx])
                    else:
                        vals.append(r.get(c, None))

                # Try insert; ignore duplicates via OR IGNORE (e.g., msg_id unique)
                try:
                    cur.execute(insert_sql, vals)
                    changed = (conn.total_changes - before) > 0
                    if changed:
                        ok_ids.append(msg_ids[idx])
                        inserted_count += 1
                        # fast stats: prefer seconds, else ms->s if numeric, else date text
                        if "timestamp_UTC_s" in r and r["timestamp_UTC_s"] is not None:
                            ts = float(r["timestamp_UTC_s"])
                            batch_min_ts = ts if batch_min_ts is None else min(batch_min_ts, ts)
                            batch_max_ts = ts if batch_max_ts is None else max(batch_max_ts, ts)
                        elif "timestamp_UTC_ms" in r and r["timestamp_UTC_ms"] is not None:
                            try:
                                tsms = float(r["timestamp_UTC_ms"])
                                ts = tsms / 1000.0
                                batch_min_ts = ts if batch_min_ts is None else min(batch_min_ts, ts)
                                batch_max_ts = ts if batch_max_ts is None else max(batch_max_ts, ts)
                            except (TypeError, ValueError):
                                pass
                        if "date" in r and r["date"] is not None:
                            d = r["date"]
                            batch_min_date = d if batch_min_date is None else min(batch_min_date, d)
                            batch_max_date = d if batch_max_date is None else max(batch_max_date, d)
                except Exception:
                    # Row failed; continue
                    pass

            conn.commit()
        except Exception:
            conn.rollback()
            raise

        # 5) Update fast table stats (row_count, min/max ranges) in O(1)
        try:
            if inserted_count > 0:
                # Initialize stats row if missing
                cur.execute('INSERT OR IGNORE INTO "__table_stats__"(table_name) VALUES (?)', (table_name,))
                # row_count += inserted_count
                cur.execute(
                    'UPDATE "__table_stats__" SET row_count = row_count + ?, updated_utc = '
                    "CURRENT_TIMESTAMP WHERE table_name = ?",
                    (inserted_count, table_name),
                )
                # min/max timestamp_utc_s
                if batch_min_ts is not None:
                    cur.execute(
                        'UPDATE "__table_stats__" SET min_timestamp_utc_s = COALESCE(min(min_timestamp_utc_s, ?), ?), '
                        "updated_utc = CURRENT_TIMESTAMP WHERE table_name = ?",
                        (batch_min_ts, batch_min_ts, table_name),
                    )
                if batch_max_ts is not None:
                    cur.execute(
                        'UPDATE "__table_stats__" SET max_timestamp_utc_s = COALESCE(max(max_timestamp_utc_s, ?), ?), '
                        "updated_utc = CURRENT_TIMESTAMP WHERE table_name = ?",
                        (batch_max_ts, batch_max_ts, table_name),
                    )
                # min/max date (lexicographic if YYYY-MM-DD)
                if batch_min_date is not None:
                    cur.execute(
                        'UPDATE "__table_stats__" SET min_date = CASE WHEN min_date IS NULL OR ? < min_date THEN ? '
                        "ELSE min_date END, updated_utc = CURRENT_TIMESTAMP WHERE table_name = ?",
                        (batch_min_date, batch_min_date, table_name),
                    )
                if batch_max_date is not None:
                    cur.execute(
                        'UPDATE "__table_stats__" SET max_date = CASE WHEN max_date IS NULL OR ? > max_date THEN ? '
                        "ELSE max_date END, updated_utc = CURRENT_TIMESTAMP WHERE table_name = ?",
                        (batch_max_date, batch_max_date, table_name),
                    )
                conn.commit()
        except Exception:
            conn.rollback()
            raise

        self.close()

        return ok_ids

    def close(self, *, try_cleanup: bool = False) -> None:
        """
        Close the current connection. If try_cleanup=True, and there are no active readers,
        flip to DELETE mode and remove -wal/-shm for a clean directory.
        """
        conn = self._conn
        db_path = self._open_for[0] if self._open_for else None

        def _no_readers(c: sqlite3.Connection) -> bool:
            try:
                c.execute("PRAGMA busy_timeout=0")  # non-blocking probe
                c.execute("BEGIN EXCLUSIVE")
                c.execute("ROLLBACK")
                return True
            except sqlite3.OperationalError:
                return False
            finally:
                try:
                    c.execute("PRAGMA busy_timeout=5000")
                except Exception:
                    pass

        if conn is not None:
            try:
                # 1) Checkpoint and try to shrink WAL
                busy = 1
                try:
                    busy, log, ckpt = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
                except Exception:
                    logger.debug("wal_checkpoint(TRUNCATE) failed or unavailable", exc_info=True)

                # 2) Optional cleanup only if no readers blocked the checkpoint
                if try_cleanup and busy == 0 and _no_readers(conn):
                    try:
                        mode = conn.execute("PRAGMA journal_mode=DELETE").fetchone()[0].lower()
                        if mode == "delete" and db_path:
                            for suf in ("-wal", "-shm"):
                                p = Path(str(db_path) + suf)
                                try:
                                    p.unlink()
                                except FileNotFoundError:
                                    pass
                                except Exception:
                                    logger.debug("Could not remove %s", p, exc_info=True)
                    except Exception:
                        logger.debug("Cleanup skipped; likely readers present", exc_info=True)
            finally:
                try:
                    if self._cursor:
                        self._cursor.close()
                finally:
                    try:
                        conn.close()
                    except Exception:
                        pass

        # Clear internal state regardless
        self._cursor = None
        self._conn = None
        self._open_for = None


class SQLiteReader:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA busy_timeout=5000;")  # avoid 'database is locked'
        self.conn.execute("PRAGMA synchronous=NORMAL;")  # good perf, safe with WAL
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()

    def get_table_rowcount(self, table_name: str) -> int:
        query = f'SELECT COUNT(*) FROM "{table_name}"'
        self.cursor.execute(query)
        row_count = self.cursor.fetchone()[0]
        return row_count

    def list_tables(self) -> list[str]:
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
        return [row["name"] for row in self.cursor.fetchall()]

    def fetch_all(self, table_name: str) -> list[dict[str, Any]]:
        query = f'SELECT * FROM "{table_name}"'
        self.cursor.execute(query)
        return [dict(row) for row in self.cursor.fetchall()]

    def fetch_where(self, table_name: str, where_clause: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        query = f'SELECT * FROM "{table_name}" WHERE {where_clause}'
        self.cursor.execute(query, params or [])
        return [dict(row) for row in self.cursor.fetchall()]

    def fetch_columns(self, table_name: str, columns: list[str], limit: int | None = None) -> list[dict[str, Any]]:
        col_str = ", ".join(f'"{col}"' for col in columns)
        query = f'SELECT {col_str} FROM "{table_name}"'
        if limit is not None:
            query += f" LIMIT {limit}"
        self.cursor.execute(query)
        return [dict(row) for row in self.cursor.fetchall()]

    def fetch_metadata(self) -> list[dict[str, Any]]:
        return self.fetch_all("stream_metadata")

    def execute_raw_query(self, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        self.cursor.execute(sql, params or [])
        return [dict(row) for row in self.cursor.fetchall()]
