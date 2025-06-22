# Set shell for Windows (PowerShell) or cross-platform logic if needed
set shell := ["powershell", "-Command"]

description:
  @echo 'This file is used to run all automated commands!'

PYTHON_VERSION := `cat .python-version`

# Install all dependencies (main + dev) into .venv
install-dev:
    chmod +x scripts/install-dev.sh
    ./scripts/install-dev.sh

# Run all key checks (including types)
format: format-code mypy

# Only fix code formatting and linting (not types)
format-code: ruff-check-fix ruff-format

ruff-format:
    uv run ruff format src tests

ruff-check:
    uv run ruff check src tests

ruff-check-fix:
    uv run ruff check src tests --fix

mypy:
    uv run mypy

test:
    uv run pytest

# Generate a repo structure documentation file
generate-structure-doc:
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
    docker build --build-arg PYTHON_VERSION={{PYTHON_VERSION}} -t stockops .
