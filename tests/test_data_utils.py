import sqlite3
from pathlib import Path

import pytest

from stockops.data.sql_db import WriterRegistry

TEST_DB = Path("test_async_writer.db")
TEST_TABLE = "test_table"


@pytest.mark.asyncio
async def test_async_writer_insert(tmp_path):
    # Use tmp_path from pytest for isolation
    db_path = tmp_path / TEST_DB

    # Write test data
    writer = WriterRegistry.get_writer(db_path, TEST_TABLE)
    await writer.write({"symbol": "AAPL", "price": "198.55", "volume": "1000"})
    await WriterRegistry.shutdown_all()

    # Verify data was written
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(f'SELECT * FROM "{TEST_TABLE}"')
    rows = cursor.fetchall()

    assert len(rows) == 1
    assert rows[0][0] == "AAPL"
    assert rows[0][1] == "198.55"
    assert rows[0][2] == "1000"

    conn.close()
