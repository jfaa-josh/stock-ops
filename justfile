set shell := ["bash", "-cu"]

description:
  @echo 'This file is used to run all automated commands!'

PYTHON_VERSION := `cat .python-version`

# Install all dependencies (main + dev) into .venv
install-dev:
    chmod +x scripts/install-dev.sh
    ./scripts/install-dev.sh

# Run all key checks (including types), not fixing any issues (for ci only)
lint-ci: ruff-check mypy

ruff-format:
    uv run ruff format src tests

ruff-check:
    uv run ruff check src tests

mypy:
    uv run mypy

test:
    uv run pytest -vv

# If data/ exists, delete everything except data/test_data/inputs/
clean-data:
    [ ! -d data ] || find data -mindepth 1 \
    \( -path data/test_data/inputs -o -path 'data/test_data/inputs/*' \) -prune -o \
    -exec rm -rf {} +

# Generate a Dockerfile for the project
docker-build:
  chmod +x scripts/derive_env_from_pyproject.py
  ./scripts/derive_env_from_pyproject.py
  test -f .env || (echo ".env not found after derive_env_from_pyproject.py" && exit 1)
  docker compose build
  if [ "${CI:-}" = "true" ]; then rm .env; fi
