"""Created on Sun Jun 15 17:18:28 2025

@author: JoshFody
"""

from pathlib import Path

# root level
ROOT_DIR = Path(__file__).resolve().parents[3]

# src level
SRC_DIR = ROOT_DIR / "src"

# stockops level
PKG_DIR = SRC_DIR / "stockops"

## Data Directory
DATA_DIR = ROOT_DIR / "data"

DATA_DB_DATESTR = "%Y-%m-%d_%H%M%S"

### Streaming
RAW_STREAMING_DIR = DATA_DIR / "raw" / "streaming"

### Historical
RAW_HISTORICAL_DIR = DATA_DIR / "raw" / "historical"
