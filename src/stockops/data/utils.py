from datetime import datetime
from pathlib import Path


def get_db_filepath(
    data_type: str = "stream_data", datestr_fmt: str = "%Y-%m-%d_%H%M%S", db_path: Path = Path(".")
) -> Path:
    """
    Generate a file path for a SQLite database with a timestamp.

    Args:
        table_name (str): Base name of the table or data source.
        datestr_fmt (str): Datetime format string for timestamp.
        db_path (Path): Directory in which to place the file.

    Returns:
        Path: Full path to the generated database file.
    """
    timestamp = datetime.now().strftime(datestr_fmt)
    db_filename = f"{data_type}_{timestamp}.db"
    return db_path / db_filename
