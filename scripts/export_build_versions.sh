#!/bin/sh
set -eu

PYTHON_VERSION="$(cut -d. -f1-2 .python-version)"
PREFECT_VERSION="$(python - <<'PY'
import tomllib
with open("pyproject.toml", "rb") as f:
    config = tomllib.load(f)
for dep in config["project"]["dependencies"]:
    if dep.startswith("prefect"):
        parts = dep.split("==")
        if len(parts) == 2:
            print(parts[1])
            raise SystemExit(0)
raise SystemExit(1)
PY
)"

if [ "${1:-}" = "--github-env" ]; then
  echo "PYTHON_VERSION=${PYTHON_VERSION}"
  echo "PREFECT_VERSION=${PREFECT_VERSION}"
else
  echo "PYTHON_VERSION=${PYTHON_VERSION}"
  echo "PREFECT_VERSION=${PREFECT_VERSION}"
fi
