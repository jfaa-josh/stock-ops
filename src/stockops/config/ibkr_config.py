import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# root level
ROOT_DIR = Path(__file__).resolve().parents[3]

# Access secrets or other data via root .env (except if in production a.k.a. Docker)
if os.getenv("ENV") != "production":
    from dotenv import load_dotenv

    dotenv_path = ROOT_DIR / ".env"
    load_dotenv(dotenv_path, override=False)

# Placeholder only — IBKR connectivity is not yet implemented.
IBKR_API_TOKEN = os.getenv("IBKR_API_TOKEN") or None
if not IBKR_API_TOKEN:
    logger.info("IBKR_API_TOKEN not set. Check .env or GitHub Secrets if it was intended.")
