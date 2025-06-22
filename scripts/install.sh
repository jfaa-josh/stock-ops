#!/usr/bin/env bash
set -euo pipefail

echo "[install.sh] Creating virtual environment and syncing core dependencies..."
uv venv .venv
uv sync --strict
