set shell := ["bash", "-cu"]

description:
  @echo 'This file is used to run all automated commands!'

PYTHON_VERSION := `cat .python-version`

# Install all dependencies (main + dev) into .venv
install-dev:
    chmod +x scripts/install-dev.sh
    ./scripts/install-dev.sh

# Run all key checks (including types), not fixing any issues (for ci only)
lint-ci: ruff-format ruff-check mypy

ruff-format:
    uv run ruff format src tests

ruff-check:
    uv run ruff check src tests

mypy:
    uv run mypy

test:
    PYTHONPATH=src uv run pytest --cov=src --cov-report=term-missing --log-cli-level=INFO tests

generate-structure-doc:
  if ! command -v tree &> /dev/null; then \
    echo "'tree' not found. Installing..."; \
    sudo apt-get update && sudo apt-get install -y tree; \
  fi
  mkdir -p docs
  echo "# Repository Structure" > docs/structure.md
  echo "" >> docs/structure.md

  echo "## Top-Level Layout" >> docs/structure.md
  ls -1 | sed 's/^/├── /' >> docs/structure.md

  echo "" >> docs/structure.md
  echo "## Source Code Structure (src/)" >> docs/structure.md
  echo '```' >> docs/structure.md
  tree src -a >> docs/structure.md
  echo '```' >> docs/structure.md

# Generate a Dockerfile for the project
docker-build:
  chmod +x scripts/derive_env_from_pyproject.py
  ./scripts/derive_env_from_pyproject.py
  test -f .env || (echo ".env not found after derive_env_from_pyproject.py" && exit 1)
  docker compose build controller postgres prefect-server prefect-agent
  if [ "${CI:-}" = "true" ]; then rm .env; fi
