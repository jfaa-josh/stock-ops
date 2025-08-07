"""Created on Sun Jun 15 17:18:28 2025

@author: JoshFody
"""

import asyncio
import logging
import sqlite3
from pathlib import Path
from typing import Any

# MAKE SURE I SAVE INTERVAL LENGTH TO THE DB FOR HISTORICAL!!!
# intraday_{self.provider}_{year_str}_{month_str}:
#     table = ticker name (in format SPY.US)
#     columns =

#         *REDOING db file naming and organization schema: (TARGET MAX FILESIZE ~500MB)
#   - CURRENT:  streaming is ~20MB for open hours, 1 ticker, 2 tables, total ~500k rows
#                    (for FULL API output)
#   - add API source to metadata...and generally rethink what is going into metadata...what
#               matters?  What is it used for? (see onenote)
#   - I NEED TO INDEX MY MOST COMMON QUERIED VARS: (prob ticker, interval, timestamp).
#               See one note on indexing.
#   1. Standardize what data I save for historical and also for streaming data...only
#               save what I need.
#   2. Streaming data should be saved in .db file by date and probably EODHD,
#           tables  = ticker name.  Combine trades and quotes. (NEED TO THINK ABOUT NA COLS).
#      - Can save ~ 25 tickers per day b4 data cap
#   3. Intraday data should be saved by MONTH and probably EODHD. Tables = ticker name.
#           (WHAT ABOUT PULL DATE/REVISION?)
#      - Can save ~15 tickers per month b4 data cap
#   4. Interday data should be saved all in 1 file probably by EODHD.  Tables = ticker name.
#       (WHAT ABOUT PULL DATE/REVISION?)
#      - Can save ~625 tickers b4 data cap


class SQLiteWriter:
    def __init__(self, db_filepath: Path, table_name: str):
        self.conn = sqlite3.connect(db_filepath)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self.table_name = table_name
        self.db_filepath = db_filepath

    def initialize_schema(self, data: dict[str, Any]) -> None:
        """
        Ensure that the target table exists with columns derived from the keys of `data`.
        SQLite will not recreate the table if it already exists.
        """
        columns = [f'"{k}" TEXT' for k in data.keys()]
        col_defs = ", ".join(columns)
        create_stmt = f'CREATE TABLE IF NOT EXISTS "{self.table_name}" ({col_defs})'
        self.cursor.execute(create_stmt)
        self.conn.commit()

    def insert(self, data: dict[str, Any]) -> None:
        """
        Insert a single row into the table. The schema is initialized dynamically.
        """
        if not isinstance(data, dict):
            raise TypeError(f"SQLiteWriter.insert expected dict, got {type(data)}")

        self.initialize_schema(data)

        keys = list(data.keys())
        values = [str(data[k]) for k in keys]
        placeholders = ", ".join("?" for _ in keys)
        column_names = ", ".join(f'"{k}"' for k in keys)
        insert_stmt = f'INSERT INTO "{self.table_name}" ({column_names}) VALUES ({placeholders})'
        self.cursor.execute(insert_stmt, values)
        self.conn.commit()

    def close(self) -> None:
        self.conn.commit()
        self.conn.close()


class AsyncSQLiteWriter:
    """
    An async-safe wrapper around SQLiteWriter that queues writes and processes
    them in a dedicated background task. Ensures that all writes to a given
    (db_path, table_name) pair are serialized.
    """

    def __init__(self, db_path: Path, table_name: str):
        self.db_path = db_path
        self.table_name = table_name
        self.queue: asyncio.Queue[Any] = asyncio.Queue()
        self.writer = SQLiteWriter(db_path, table_name)
        self._task = asyncio.create_task(self._writer_loop())
        self._shutdown = False

    async def write(self, row: dict[str, Any]) -> None:
        if self._shutdown:
            raise RuntimeError("Writer has been shut down.")
        await self.queue.put(row)

    async def shutdown(self) -> None:
        self._shutdown = True
        await self.queue.put(None)  # Sentinel to end the loop
        await self._task

    async def _writer_loop(self) -> None:
        while True:
            row = await self.queue.get()
            if row is None:
                break
            try:
                self.writer.insert(row)
            except Exception as e:
                logging.error(f"[{self.table_name}] Failed to insert row: {e}")
        self.writer.close()


class SQLiteReader:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
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

    def close(self):
        self.conn.close()
