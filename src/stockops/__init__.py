"""Top-level package for StockOps."""

# All imports must go before any code execution (incl. logging setup!)
import logging

from . import config as config
from . import data as data

__version__ = "0.1.0"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

__all__ = [
    "__version__",
    "config",
    "data",
]
