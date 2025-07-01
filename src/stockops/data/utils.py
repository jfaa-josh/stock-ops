from datetime import date
from pathlib import Path

from stockops.config import DATA_DB_DATESTR, RAW_STREAMING_DIR


def get_streaming_db_path(db_date: date | None = None) -> Path:
    """
    Returns the path to the .db file for the given date.
    If no date is given, uses today's date.
    """
    if db_date is None:
        db_date = date.today()
    db_name = db_date.strftime(DATA_DB_DATESTR) + ".db"
    return RAW_STREAMING_DIR / db_name
