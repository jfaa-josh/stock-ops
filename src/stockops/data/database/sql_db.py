from __future__ import annotations

import json
import logging
import sqlite3
from collections import defaultdict
from decimal import Decimal
from numbers import Integral, Real
from pathlib import Path
from typing import Any

import numpy as np

from stockops.config import utils as cfg_utils  # , add additional providers here as needed
from stockops.data.database.utils import set_ts_col
from stockops.data.utils import normalize_ts_to_seconds, validate_isodatestr

logger = logging.getLogger(__name__)

IndexMode = ["historical_intraday", "streaming", "historical_interday"]
# historical_intraday: ("timestamp_UTC_s", "interval")
# streaming: ("timestamp_UTC_ms",)
# historical_interday: ("date", "interval")


class SQLiteWriter:
    def __init__(self):
        self._conn: sqlite3.Connection | None = None
        self._cursor: sqlite3.Cursor | None = None
        self._open_for: tuple[Path, str] | None = None
        self._table_meta: dict[tuple[Path, str], dict[str, Any]] = {}
        self._open_db: Path | None = None

    def update_min_max(self, val, mn, mx):
        """Update min/max with a single comparison each (no branching)."""
        if val is None:
            return mn, mx
        mn = val if mn is None else (val if val < mn else mn)
        mx = val if mx is None else (val if val > mx else mx)
        return mn, mx

    # ---------- Type inference ----------
    def infer_sqlite_affinity(self, val: Any) -> str:
        if val is None:
            return "TEXT"
        if isinstance(val, (bytes | bytearray | memoryview)):
            return "BLOB"
        if isinstance(val, (bool | np.bool_)):
            return "INTEGER"
        if isinstance(val, (Integral | np.integer)):
            return "INTEGER"
        if isinstance(val, Decimal):
            return "NUMERIC"
        if isinstance(val, (Real | np.floating)):
            return "REAL"
        return "TEXT"

    # ---------- Index mode helpers ----------
    def _set_index_from_mode(self, provider: str, mode: str) -> list[str]:
        self.ts_col = set_ts_col(provider, mode)

        if mode in ["historical_interday", "historical_intraday"]:
            return [self.ts_col, "interval"]
        elif mode == "streaming":
            return [self.ts_col]
        else:
            raise ValueError(f"Mode {mode} not recognized.")

    def _migrate_table_schema(
        self, conn: sqlite3.Connection, cur: sqlite3.Cursor, table_name: str, mode: str, idx_cols: list[str]
    ) -> bool:
        """
        If existing declared column types are weaker/mismatched and we now have
        stronger desired affinities, rebuild the table with the new declarations.

        Returns True if a migration was performed.
        """
        # Load current schema
        cur.execute(f'PRAGMA table_info("{table_name}")')
        info = cur.fetchall()
        existing_cols = [row["name"] for row in info]
        existing_decl = {row["name"]: (row["type"] or "").upper() for row in info}

        # Columns we never rewrite
        protected = set(idx_cols) | {"version"}

        # Decide which columns need upgrading
        to_upgrade: dict[str, str] = {}
        for col, want in (self.target_affinity or {}).items():
            if col in protected:
                continue
            if want is None:
                # still unknown → no upgrade yet
                continue
            have_u = existing_decl.get(col, "")
            if col in existing_decl:
                # Rewrite if declared type is empty (NONE) or TEXT/NUMERIC and differs from target.
                if have_u != want:
                    to_upgrade[col] = want

        if not to_upgrade:
            return False

        # Build new table DDL: keep order as existing, with upgraded types for the chosen columns.
        def coldef(name: str) -> str:
            if name in idx_cols:
                return f"{name} {self.target_affinity[name]} NOT NULL"
            if name == "version":
                return '"version" INTEGER NOT NULL DEFAULT 1'
            # data columns
            tgt = to_upgrade.get(name)
            if tgt:
                return f'"{name}" {tgt}'
            # keep original declaration (may be NONE/"")
            have = existing_decl.get(name, "")
            if have:
                return f'"{name}" {have}'
            else:
                # NONE affinity = declare without a type
                return f'"{name}"'

        new_cols_defs = ", ".join(coldef(c) for c in existing_cols)
        tmp_name = f"__tmp_{table_name}"

        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                # 1) Create tmp table with upgraded DDL
                cur.execute(f'CREATE TABLE "{tmp_name}" ({new_cols_defs})')

                # 2) Copy data
                col_list_sql = ", ".join(f'"{c}"' for c in existing_cols)
                cur.execute(f'INSERT INTO "{tmp_name}" ({col_list_sql}) SELECT {col_list_sql} FROM "{table_name}"')

                # 3) Drop old and rename tmp
                cur.execute(f'DROP TABLE "{table_name}"')
                cur.execute(f'ALTER TABLE "{tmp_name}" RENAME TO "{table_name}"')

                # 4) Recreate indexes
                cur.execute(
                    f'CREATE INDEX IF NOT EXISTS "ix_{table_name}_" ON "{table_name}" '
                    f"({', '.join(f'"{c}"' for c in idx_cols)})"
                )
                cur.execute(
                    f'CREATE UNIQUE INDEX IF NOT EXISTS "ux_{table_name}_ver" '
                    f'ON "{table_name}" ({", ".join(f'"{c}"' for c in idx_cols)},"version")'
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

        except sqlite3.OperationalError as e:
            # If locked (viewer/reader present), skip migration now to avoid stalls.
            msg = str(e).lower()
            if "locked" in msg or "busy" in msg:
                try:
                    conn.rollback()
                except Exception:
                    pass
                logger.debug("Skipping schema migration for %s (db is busy). Will retry later.", table_name)
                return False
            raise

        # refresh cache
        cur.execute(f'PRAGMA table_info("{table_name}")')
        if self._open_for is None:
            raise RuntimeError("Failed to initialize _open_for")
        self._table_meta[self._open_for] = {
            "mode": mode,
            "cols": {row["name"] for row in cur.fetchall()},
        }
        return True

    def _verify_or_create_tables(
        self,
        cur: sqlite3.Cursor,
        table_name: str,
        idx_cols: list[str],
        provider: str,
        exchange: str,
        db_just_opened: bool,
        mode: str,
    ) -> None:
        # Ensure meta & stats tables exist (DB-level)
        cur.execute('CREATE TABLE IF NOT EXISTS "__meta__" (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
        cur.execute(
            'CREATE TABLE IF NOT EXISTS "__table_stats__" ('
            "  table_name TEXT PRIMARY KEY,"
            "  row_count INTEGER NOT NULL DEFAULT 0,"
            "  min_timestamp_utc_s REAL,"
            "  max_timestamp_utc_s REAL,"
            "  min_date TEXT,"
            "  max_date TEXT,"
            "  updated_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
        )
        if mode != "streaming":
            cur.execute(
                'CREATE TABLE IF NOT EXISTS "__interval_stats__" ('
                "  table_name TEXT NOT NULL,"
                "  interval   TEXT NOT NULL,"
                "  row_count  INTEGER NOT NULL DEFAULT 0,"
                "  min_timestamp_utc_s REAL,"
                "  max_timestamp_utc_s REAL,"
                "  min_date TEXT,"
                "  max_date TEXT,"
                "  updated_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                "  PRIMARY KEY (table_name, interval)"
                ")"
            )

        if db_just_opened:
            try:
                # mimic old code: get metadata from cfg_utils
                meta = cfg_utils.ProviderConfig(provider, exchange).cfg.EXCHANGE_METADATA[exchange]
            except Exception:
                logger.warning("Skipping provider metadata seeding; parse/lookup failed.", exc_info=True)
                meta = {}

            rows = [(str(k), json.dumps(v) if isinstance(v, dict) else str(v)) for k, v in meta.items()]
            rows.extend(
                [
                    ("provider", provider),
                    ("exchange", exchange),
                ]
            )
            cur.executemany('INSERT OR REPLACE INTO "__meta__"(key, value) VALUES (?, ?)', rows)
            # Fix created_utc to real timestamp
            cur.execute('UPDATE "__meta__" SET value = CURRENT_TIMESTAMP WHERE key = "created_utc"')

        # Create data table if missing
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
        exists = cur.fetchone() is not None

        if not exists:
            col_defs = []  # Create table with typed index columns
            for c in idx_cols:
                col_defs.append(f"{c} {self.target_affinity[c]} NOT NULL")
            col_defs.append('"version" INTEGER NOT NULL DEFAULT 1')
            cur.execute(f'CREATE TABLE "{table_name}" ({", ".join(col_defs)})')

            # indexes
            cur.execute(
                f'CREATE INDEX IF NOT EXISTS "ix_{table_name}_" ON "{table_name}" '
                f"({', '.join(f'"{c}"' for c in idx_cols)})"
            )
            cur.execute(
                f'CREATE UNIQUE INDEX IF NOT EXISTS "ux_{table_name}_ver" '
                f'ON "{table_name}" ({", ".join(f'"{c}"' for c in idx_cols)},"version")'
            )

    def _ensure_open(
        self, db_filepath: Path, table_name: str, mode: str, idx_cols: list[str], provider: str, exchange: str
    ) -> None:
        """Open DB; create/verify meta, stats, and the table with the requested mode."""
        target = (db_filepath, table_name)
        need_open = (self._open_db != db_filepath) or (self._conn is None) or (self._cursor is None)
        if need_open:
            logger.info("Opening DB %s; table=%s; mode=%s; idx_cols=%s", db_filepath, table_name, mode, idx_cols)

            self.close()
            db_filepath.parent.mkdir(parents=True, exist_ok=True)
            uri = f"file:{str(db_filepath)}?mode=rwc"
            conn = sqlite3.connect(uri, uri=True, isolation_level=None, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout=5000;")
            jcur = conn.execute("PRAGMA journal_mode;")
            journal_mode = jcur.fetchone()[0].upper()
            if journal_mode != "WAL":
                conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA wal_autocheckpoint=1000;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            cur = conn.cursor()
            self._conn, self._cursor, self._open_db = conn, cur, db_filepath

        if self._cursor is None:
            raise RuntimeError("Failed to initialize cursor")

        if self._conn is None:
            raise RuntimeError("Database connection was not initialized (self._conn is None).")

        cur = self._cursor

        # Ensure the target table exists even if the DB was already open
        cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1;", (table_name,))
        if cur.fetchone() is None:
            # DB wasn't "just opened", we're just adding a new table to an existing DB
            self._verify_or_create_tables(
                cur, table_name, idx_cols, provider, exchange, db_just_opened=False, mode=mode
            )
            self._conn.commit()

        # Verify table shape & cache for index columns
        cur.execute(f'PRAGMA table_info("{table_name}")')
        existing_cols = {row["name"] for row in cur.fetchall()}
        required = set(idx_cols) | {"version"}
        if not required.issubset(existing_cols):
            raise RuntimeError(
                f'Table "{table_name}" does not match requested index-mode {mode}; '
                f"missing required columns {sorted(required - existing_cols)}."
            )

        self._open_for = target
        self._table_meta[target] = {"mode": mode, "cols": existing_cols}

    def _evolve_columns(self, cur: sqlite3.Cursor, table_name: str, needed_cols: set[str]) -> None:
        """
        Add new columns with requested affinities.
        - self.target_affinity[col] == None  -> declare without a type (affinity NONE)
        - self.target_affinity[col] == 'REAL'/'INTEGER'/... -> declare with that type
        If column already exists, leave as-is (log if different).
        """
        cur.execute(f'PRAGMA table_info("{table_name}")')
        info = cur.fetchall()
        existing = {row["name"]: row for row in info}
        to_add = [c for c in needed_cols if c not in existing and c != "version"]

        # Add new columns
        for c in to_add:
            aff = self.target_affinity.get(c)
            if aff is None:
                # declare WITHOUT a type -> affinity NONE
                cur.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{c}"')
            else:
                cur.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{c}" {aff}')
        if to_add:
            cur.connection.commit()
            key = self._open_for
            assert key is not None, "SQLiteWriter._open_for not initialized"
            self._table_meta[key]["cols"] |= set(to_add)

        # Warn when inferred type disagrees with declared type (purely informational; we do not rewrite)
        if self.target_affinity:
            cur.execute(f'PRAGMA table_info("{table_name}")')
            after = {row["name"]: row for row in cur.fetchall()}
            for c in needed_cols:
                if c in after and c in self.target_affinity:
                    declared = (after[c]["type"] or "").upper()  # may be '' when NONE
                    inferred = (self.target_affinity[c] or "").upper()
                    if declared and inferred and declared != inferred:
                        logger.debug(
                            'Column "%s" declared as %s but inferred %s (leaving as-is).', c, declared, inferred
                        )

    def insert_many(
        self,
        db_filepath: Path,
        table_name: str,
        batch: list[tuple[str, dict[str, Any]]],
        provider: str,
        exchange: str,
        mode: str,
    ) -> list[str]:
        """
        Batch insert with dedup/version semantics.

        - Homogeneous index mode across the batch (validated here).
        - 'msg_id' is required in payload for ACKs but is NOT stored.
        - Dedup:
            1) Subset by index exact match (via index).
            2) If any row in subset matches all payload keys (ignoring DB-only cols), skip (dup) but ACK.
            3) Else insert with version = max(existing.version)+1. If subset empty, version=1.
        """
        assert mode in IndexMode, "Unknown data_type passed as mode to sql_db.insert_many()"
        if not batch:
            return []

        # Validate homogeneous table and db_filepath across batch
        idx_cols = self._set_index_from_mode(provider, mode)  # Consistency checks have been done in writer.py

        # Track interval stats
        track_intervals = mode != "streaming"
        interval_tally: defaultdict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "count": 0,
                "min_ts": None,
                "max_ts": None,
                "min_date": None,
                "max_date": None,
            }
        )

        check_all = all(d.get("db_path") == str(db_filepath) and d.get("table") == table_name for _, d in batch)
        if not check_all:
            raise ValueError("Mixed index configurations within the same batch are not allowed.")

        # Evolve data columns (non-index, non-version, non-msg_id) with type inference from sample values
        affinity_cols = set().union(*(p["row"].keys() for _, p in batch))

        first_non_null: dict[str, Any] = dict.fromkeys(affinity_cols)
        for _, dict_p in batch:
            p = dict_p["row"]
            for c in affinity_cols:
                if first_non_null[c] is None:
                    v = p.get(c)
                    if v is not None:
                        first_non_null[c] = v

        # Decide the target declared type per column (affinity)
        # None -> declare with no type (affinity NONE)
        self.target_affinity: dict[str, str | None] = {}
        for c in affinity_cols:
            v = first_non_null[c]
            if v is not None:
                self.target_affinity[c] = self.infer_sqlite_affinity(v)
        payload_cols = affinity_cols - (set(idx_cols) | {"version"})  # All non None

        # Skip and ack if all payload values are None, or contain only index cols
        ok_ids: list[str] = []
        if not payload_cols or all(first_non_null.get(c) is None for c in payload_cols):
            ok_ids = [mid for mid, _ in batch]
            logger.debug(
                "Index-only batch for %s/%s (mode=%s); acking %d message(s) without DB I/O",
                db_filepath,
                table_name,
                mode,
                len(ok_ids),
            )
            return ok_ids

        # Open / verify schema for this mode
        self._ensure_open(db_filepath, table_name, mode, idx_cols, provider, exchange)
        if self._cursor is None or self._conn is None:
            raise RuntimeError("Failed to initialize cursor, conn, or target")
        conn, cur = self._conn, self._cursor

        # If existing table contains col with no type, but current batch can be used to type col...
        # Opportunistic migration for already-existing weak declarations
        # Note: this works because previously untyped cols are unlikely to be large tables
        self._migrate_table_schema(conn, cur, table_name, mode=mode, idx_cols=idx_cols)

        # Then evolve columns for any *new* columns (those not yet present)
        self._evolve_columns(cur, table_name, payload_cols)

        # Prepare SELECT for index subset
        where_sql = " AND ".join([f'"{c}" = ?' for c in idx_cols])
        select_subset_sql = f'SELECT * FROM "{table_name}" WHERE {where_sql};'

        # Prepare insert columns (index + payload_cols∩table + version)
        cur.execute(f'PRAGMA table_info("{table_name}")')
        table_cols = [row["name"] for row in cur.fetchall()]
        non_index_payload_cols = [c for c in table_cols if c in payload_cols]
        insert_cols = idx_cols + non_index_payload_cols + ["version"]
        placeholders = ", ".join("?" for _ in insert_cols)
        insert_sql = (
            f'INSERT INTO "{table_name}" ({", ".join(f"""\"{c}\"""" for c in insert_cols)}) VALUES ({placeholders});'
        )

        # Stats accumulation
        batch_min_ts = None  # seconds
        batch_max_ts = None
        batch_min_date = None
        batch_max_date = None

        conn.execute("BEGIN IMMEDIATE")
        try:
            for tup_in in batch:
                msg_id = tup_in[0]
                if not isinstance(msg_id, str) or not msg_id.strip():
                    raise ValueError("Each payload must include a non-empty 'msg_id' string.")

                payload = tup_in[1]["row"]

                # Test and reject empy payload
                is_blank_data = all(payload.get(k) is None for k in payload_cols)
                if is_blank_data:
                    ok_ids.append(msg_id)
                    continue

                # Build & validate index values (must be present and not None)
                for c in idx_cols:
                    if c not in payload or payload[c] is None:
                        raise ValueError(f"Missing required index column '{c}' (mode={mode})")
                idx_vals = tuple(payload[c] for c in idx_cols)

                ### Check for dupes
                # Subset by index
                cur.execute(select_subset_sql, idx_vals)
                subset_rows = cur.fetchall()
                logger.debug("subset_rows=%d for idx=%s", len(subset_rows), idx_vals)

                version = 1
                if subset_rows:
                    sentinel = object()
                    p_items = tuple(payload.items())
                    is_exact_dup = False
                    for r in subset_rows:
                        rd = dict(r)  # convert Row to plain dict to use .get()
                        # Test if payload has all keys and all match (exact duplicate)
                        if all(rd.get(k, sentinel) == v for k, v in p_items):
                            is_exact_dup = True
                            break

                    if is_exact_dup:
                        ok_ids.append(msg_id)
                        logger.debug("Exact duplicate detected; ack only. idx=%s", idx_vals)
                        continue

                    ### Set new version number
                    cur.execute(  # Subset row with max version number
                        f'SELECT * FROM "{table_name}" WHERE {where_sql} ORDER BY "version" DESC NULLS LAST LIMIT 1;',
                        idx_vals,
                    )
                    row_max = cur.fetchone()
                    max_version = int(row_max["version"]) if row_max and row_max["version"] is not None else 0
                    version = max_version + 1

                # Assemble values for insert
                row_vals: list[Any] = []
                row_vals.extend(payload.get(c) for c in idx_cols)  # index
                row_vals.extend(payload.get(c) for c in non_index_payload_cols)  # data
                row_vals.append(int(version))  # version
                if version > 1:
                    logger.debug("Insert with version=%d idx=%s", version, idx_vals)

                try:
                    cur.execute(insert_sql, row_vals)
                except Exception:
                    logger.exception(
                        "Row insert failed for %s/%s idx=%s; skipping row", db_filepath, table_name, idx_vals
                    )
                    ok_ids.append(msg_id)
                    continue
                ok_ids.append(msg_id)

                # Update tally for row interval stats
                if track_intervals:
                    iv = payload.get("interval")
                    if iv is not None:
                        s = interval_tally[str(iv)]
                        s["count"] += 1
                        v = payload.get(self.ts_col)

                        if isinstance(v, int):
                            ts_sec = normalize_ts_to_seconds(v)
                            s["min_ts"], s["max_ts"] = self.update_min_max(ts_sec, s["min_ts"], s["max_ts"])
                        elif isinstance(v, str):
                            d = validate_isodatestr(v)
                            s["min_date"], s["max_date"] = self.update_min_max(d, s["min_date"], s["max_date"])

                # Stats update inputs
                v = payload.get(self.ts_col)

                # Fast numeric path (no exceptions)
                if isinstance(v, int):
                    ts_sec = normalize_ts_to_seconds(v)
                    batch_min_ts, batch_max_ts = self.update_min_max(ts_sec, batch_min_ts, batch_max_ts)
                # String: try digit-only first (still no exceptions)
                elif isinstance(v, str):
                    d = validate_isodatestr(v)
                    batch_min_date, batch_max_date = self.update_min_max(d, batch_min_date, batch_max_date)

            conn.commit()
        except Exception as e:
            logger.exception("There was an exception on the batch.  Nothing was written -> rollback: %s", e)
            conn.rollback()
            raise

        # __table_stats__: apply O(1) updates once per batch
        try:
            if ok_ids:
                cur.execute('INSERT OR IGNORE INTO "__table_stats__"(table_name) VALUES (?)', (table_name,))
                cur.execute(
                    'UPDATE "__table_stats__" SET row_count = row_count + ?, updated_utc = CURRENT_TIMESTAMP '
                    "WHERE table_name = ?",
                    (len(ok_ids), table_name),
                )
                if batch_min_ts is not None:
                    cur.execute(
                        'UPDATE "__table_stats__" SET '
                        "min_timestamp_utc_s = COALESCE(min(min_timestamp_utc_s, ?), ?), "
                        "updated_utc = CURRENT_TIMESTAMP WHERE table_name = ?",
                        (batch_min_ts, batch_min_ts, table_name),
                    )
                if batch_max_ts is not None:
                    cur.execute(
                        'UPDATE "__table_stats__" SET '
                        "max_timestamp_utc_s = COALESCE(max(max_timestamp_utc_s, ?), ?), "
                        "updated_utc = CURRENT_TIMESTAMP WHERE table_name = ?",
                        (batch_max_ts, batch_max_ts, table_name),
                    )
                if batch_min_date is not None:
                    cur.execute(
                        'UPDATE "__table_stats__" SET '
                        "min_date = CASE WHEN min_date IS NULL OR ? < min_date THEN ? ELSE min_date END, "
                        "updated_utc = CURRENT_TIMESTAMP WHERE table_name = ?",
                        (batch_min_date, batch_min_date, table_name),
                    )
                if batch_max_date is not None:
                    cur.execute(
                        'UPDATE "__table_stats__" SET '
                        "max_date = CASE WHEN max_date IS NULL OR ? > max_date THEN ? ELSE max_date END, "
                        "updated_utc = CURRENT_TIMESTAMP WHERE table_name = ?",
                        (batch_max_date, batch_max_date, table_name),
                    )
                conn.commit()
        except Exception:
            conn.rollback()
            raise

        # __interval_stats__: apply O(1) updates once per batch
        try:
            if track_intervals and interval_tally:
                conn.execute("BEGIN IMMEDIATE")
                for iv, s in interval_tally.items():
                    # Ensure row exists
                    cur.execute(
                        'INSERT OR IGNORE INTO "__interval_stats__"(table_name, interval) VALUES (?, ?)',
                        (table_name, iv),
                    )

                    # Increment count
                    if s["count"] > 0:
                        cur.execute(
                            'UPDATE "__interval_stats__" SET '
                            "row_count = row_count + ?, "
                            "updated_utc = CURRENT_TIMESTAMP "
                            "WHERE table_name = ? AND interval = ?",
                            (s["count"], table_name, iv),
                        )

                    # Merge numeric timestamp bounds (intraday)
                    if s["min_ts"] is not None:
                        cur.execute(
                            'UPDATE "__interval_stats__" SET '
                            "min_timestamp_utc_s = CASE "
                            "  WHEN min_timestamp_utc_s IS NULL OR ? < min_timestamp_utc_s THEN ? "
                            "  ELSE min_timestamp_utc_s END, "
                            "updated_utc = CURRENT_TIMESTAMP "
                            "WHERE table_name = ? AND interval = ?",
                            (s["min_ts"], s["min_ts"], table_name, iv),
                        )
                    if s["max_ts"] is not None:
                        cur.execute(
                            'UPDATE "__interval_stats__" SET '
                            "max_timestamp_utc_s = CASE "
                            "  WHEN max_timestamp_utc_s IS NULL OR ? > max_timestamp_utc_s THEN ? "
                            "  ELSE max_timestamp_utc_s END, "
                            "updated_utc = CURRENT_TIMESTAMP "
                            "WHERE table_name = ? AND interval = ?",
                            (s["max_ts"], s["max_ts"], table_name, iv),
                        )

                    # Merge date bounds (interday)
                    if s["min_date"] is not None:
                        cur.execute(
                            'UPDATE "__interval_stats__" SET '
                            "min_date = CASE "
                            "  WHEN min_date IS NULL OR ? < min_date THEN ? "
                            "  ELSE min_date END, "
                            "updated_utc = CURRENT_TIMESTAMP "
                            "WHERE table_name = ? AND interval = ?",
                            (s["min_date"], s["min_date"], table_name, iv),
                        )
                    if s["max_date"] is not None:
                        cur.execute(
                            'UPDATE "__interval_stats__" SET '
                            "max_date = CASE "
                            "  WHEN max_date IS NULL OR ? > max_date THEN ? "
                            "  ELSE max_date END, "
                            "updated_utc = CURRENT_TIMESTAMP "
                            "WHERE table_name = ? AND interval = ?",
                            (s["max_date"], s["max_date"], table_name, iv),
                        )

                conn.commit()
        except Exception:
            conn.rollback()
            raise

        return ok_ids

    def close(self) -> None:
        """
        Close cursor/connection. If `clean` and this was opened for writing,
        attempt to checkpoint WAL and switch to DELETE to remove -wal/-shm.
        """
        try:
            if self._conn is not None:
                # If not autocommit, ensure all writes are flushed
                try:
                    self._conn.commit()
                except Exception:
                    pass
        finally:
            # Close cursor/connection unconditionally
            if self._cursor is not None:
                try:
                    self._cursor.close()
                except Exception:
                    pass
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:
                    pass
            self._cursor = None
            self._conn = None
            self._open_for = None


class SQLiteReader:
    def __init__(self, ts_col: str, busy_timeout_ms: int = 5000):
        self.ts_col = ts_col
        self.busy_timeout_ms = busy_timeout_ms
        self._conn: sqlite3.Connection | None = None
        self._cursor: sqlite3.Cursor | None = None

    def read_dt_range(
        self, db_files: list[Path], table: str, interval: str | None, start: str | int, end: str | int
    ) -> list[dict]:
        """
        Return all 1m rows for table between [start, end] (inclusive) across the given files.
        Skips missing/empty files gracefully. Returns a single list of dicts sorted by datetime.
        """
        rows = []
        out: list[dict] = []
        for db in db_files:
            if not Path(db).exists():
                continue

            try:
                self._connect_ro(db)

                if self._conn is None:
                    raise

                if not self._table_exists(table):
                    continue

                if not self._has_any_in_range(table, interval, start, end):
                    continue

                rows = self._query_range(table, interval, start, end) or []
                out.extend(rows)

            except sqlite3.Error as e:
                logger.warning("SQLite error reading %s: %s", db, e)
                continue

        self.close()

        if not rows:
            return []

        out.sort(key=lambda r: r[self.ts_col])
        return out

    def _connect_ro(self, db_path: Path):
        """
        Open read-only with WAL-friendly pragmas.
        Using URI with mode=ro ensures we don’t interfere with the writer.
        """
        # read-only, don’t attempt to write WAL files
        uri = f"file:{str(db_path)}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, isolation_level=None, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms};")
        conn.execute("PRAGMA query_only=ON;")
        self._conn = conn

    def _table_exists(self, table: str) -> bool:
        q = "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;"
        if self._conn is not None:
            cur = self._conn.execute(q, (table,))
        return cur.fetchone() is not None

    def _has_any_in_range(self, table: str, interval: str | None, start: str | int, end: str | int) -> bool:
        if interval and self._col_exists(table, "interval"):
            q = f'SELECT 1 FROM "{table}" WHERE {self.ts_col} BETWEEN ? AND ? AND interval=? LIMIT 1;'
            if self._conn is not None:
                cur = self._conn.execute(q, (start, end, interval))
        else:
            q = f'SELECT 1 FROM "{table}" WHERE {self.ts_col} BETWEEN ? AND ? LIMIT 1;'
            if self._conn is not None:
                cur = self._conn.execute(q, (start, end))
        return cur.fetchone() is not None

    def _query_range(self, table: str, interval: str | None, start: str | int, end: str | int) -> list[dict]:
        if interval and self._col_exists(table, "interval"):
            q = f'SELECT * FROM "{table}" WHERE {self.ts_col} BETWEEN ? AND ? AND interval=?;'
            if self._conn is not None:
                cur = self._conn.execute(q, (start, end, interval))
        else:
            q = f'SELECT * FROM "{table}" WHERE {self.ts_col} BETWEEN ? AND ?;'
            if self._conn is not None:
                cur = self._conn.execute(q, (start, end))
        return [dict(row) for row in cur.fetchall()]

    def _col_exists(self, table: str, col: str) -> bool:
        q = f'PRAGMA table_info("{table}");'
        if self._conn is not None:
            cols = {r["name"] for r in self._conn.execute(q)}
        return col in cols

    def close(self) -> None:
        """
        Close cursor/connection for a read-only session.
        Readers should never try to checkpoint WAL or change journal_mode.
        """
        try:
            if self._cursor is not None:
                try:
                    self._cursor.close()
                except Exception:
                    pass
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:
                    pass
        finally:
            self._cursor = None
            self._conn = None
