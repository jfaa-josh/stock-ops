"""Created on Sun Jun 15 17:18:28 2025

@author: JoshFody
"""

import sqlite3
from pathlib import Path
from typing import Any


class SQLiteWriter:
    def __init__(self, db_filepath: Path, table_name: str):
        self.conn = sqlite3.connect(db_filepath, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.table_name = table_name
        self.schema_initialized = False
        self.db_filepath = db_filepath  # store for reference if needed

    def initialize_schema(self, data: dict):
        columns = [f'"{k}" TEXT' for k in data.keys()]
        col_defs = ", ".join(columns)
        create_stmt = f'CREATE TABLE IF NOT EXISTS "{self.table_name}" ({col_defs})'
        self.cursor.execute(create_stmt)
        self.conn.commit()
        self.schema_initialized = True

    def insert(self, data: dict):
        if not self.schema_initialized:
            self.initialize_schema(data)

        keys = list(data.keys())
        values = [str(data[k]) for k in keys]
        placeholders = ", ".join("?" for _ in keys)
        column_names = ", ".join(f'"{k}"' for k in keys)
        insert_stmt = f'INSERT INTO "{self.table_name}" ({column_names}) VALUES ({placeholders})'
        self.cursor.execute(insert_stmt, values)
        self.conn.commit()

    def insert_metadata(self, metadata: dict):
        meta_table = "stream_metadata"
        columns = [f'"{k}" TEXT' for k in metadata.keys()]
        col_defs = ", ".join(columns)
        create_stmt = f'CREATE TABLE IF NOT EXISTS "{meta_table}" ({col_defs})'
        self.cursor.execute(create_stmt)

        keys = list(metadata.keys())
        values = [str(metadata[k]) for k in keys]
        placeholders = ", ".join("?" for _ in keys)
        column_names = ", ".join(f'"{k}"' for k in keys)
        insert_stmt = f'INSERT INTO "{meta_table}" ({column_names}) VALUES ({placeholders})'
        self.cursor.execute(insert_stmt, values)
        self.conn.commit()

    def close(self):
        self.conn.commit()
        self.conn.close()


class SQLiteReader:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()

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
