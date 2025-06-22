#!/usr/bin/env bash
set -euo pipefail

echo "[install-dev.sh] Creating virtual environment and syncing core + dev dependencies..."
uv venv .venv
uv pip install .
