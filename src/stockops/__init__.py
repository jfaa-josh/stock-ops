# src/stockops/__init__.py

"""Top-level package for StockOps."""

__version__ = "0.1.0"

# Optional: configure default logging
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

# Expose high-level interfaces for convenience
from . import config, data, deploy, mlflow_utils, model

# Expose orchestrators or shared utilities directly
from .data.controller import run_streams as run_data_pipeline
# from .model.pipelines import train_pipeline
# from .deploy.predictor import predict_single
