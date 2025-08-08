import os
from pathlib import Path

# root level
ROOT_DIR = Path(__file__).resolve().parents[3]

# src level
SRC_DIR = ROOT_DIR / "src"

# stockops level
PKG_DIR = SRC_DIR / "stockops"


## Data Directory
def get_data_path(default_local):  # Switch for docker vs local
    return Path(os.environ.get("DB_DATA_DIR", default_local))


DATA_DIR = get_data_path(ROOT_DIR / "data")

### Streaming
RAW_STREAMING_DIR = DATA_DIR / "raw" / "streaming"

### Historical
RAW_HISTORICAL_DIR = DATA_DIR / "raw" / "historical"
