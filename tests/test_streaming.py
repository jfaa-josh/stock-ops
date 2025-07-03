from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from stockops.data.utils import get_db_filepath

# Fixed timestamp for deterministic testing
FIXED_DATETIME = datetime(2025, 7, 1, 13, 52, 45)
EXPECTED_TIMESTAMP = "2025-07-01_135245"


@patch("stockops.data.utils.datetime")  # Patch datetime inside your utils module
def test_get_stream_filepath_defaults(mock_datetime):
    mock_datetime.now.return_value = FIXED_DATETIME
    mock_datetime.strftime = datetime.strftime  # ensure strftime works

    expected_name = f"stream_data_{EXPECTED_TIMESTAMP}.db"
    expected_path = Path(".") / expected_name

    result = get_db_filepath()
    assert result == expected_path
    assert isinstance(result, Path)
    assert result.name.endswith(".db")


@patch("stockops.data.utils.datetime")
def test_get_stream_filepath_custom_args(mock_datetime):
    mock_datetime.now.return_value = FIXED_DATETIME
    mock_datetime.strftime = datetime.strftime

    table_name = "prices"
    db_path = Path("/tmp/mydata")
    fmt = "%Y-%m-%d_%H%M%S"

    expected_name = f"{table_name}_{EXPECTED_TIMESTAMP}.db"
    expected_path = db_path / expected_name

    result = get_db_filepath(data_type=table_name, datestr_fmt=fmt, db_path=db_path)
    assert result == expected_path
    assert result.parent == db_path
