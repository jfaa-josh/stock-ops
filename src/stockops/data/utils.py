from datetime import datetime
from pathlib import Path


def get_db_filepath(data_type: str, data_source: str, timestamp: None | datetime, db_path: Path = Path(".")) -> Path:
    """
    Generate a file path for a SQLite database based on data type, source, and timestamp.

    Args:
        data_type (str): "streaming", "intraday", or "interday"
        data_source (str): e.g., "EODHD"
        timestamp (datetime): Timestamp of the data row
        db_path (Path): Base directory to store DB files

    Returns:
        Path: Path to the SQLite DB file
    """
    if data_type == "streaming" and timestamp is not None:
        date_str = timestamp.strftime("%Y-%m-%d")
        filename = f"{data_type}_{date_str}_{data_source}.db"
    elif data_type == "intraday" and timestamp is not None:
        date_str = timestamp.strftime("%Y-%m")
        filename = f"{data_type}_{date_str}_{data_source}.db"
    elif data_type == "interday":
        filename = f"{data_type}_{data_source}.db"
    else:
        raise ValueError(f"Unsupported data_type: {data_type}")
    return db_path / filename
