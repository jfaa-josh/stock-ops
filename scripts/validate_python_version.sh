#!/usr/bin/env bash
set -euo pipefail

PY_VER_FILE=$(cut -d. -f1,2 .python-version)
PY_VER_TOML=$(grep -E 'requires-python\s*=' pyproject.toml | grep -o '[0-9]\+\.[0-9]\+')

if [[ "$PY_VER_FILE" != "$PY_VER_TOML" ]]; then
  echo "Version mismatch: .python-version = $PY_VER_FILE but pyproject.toml = $PY_VER_TOML"
  exit 1
fi

echo "Python version match confirmed: $PY_VER_FILE"
