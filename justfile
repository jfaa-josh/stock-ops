# use PowerShell instead of sh:
set shell := ["powershell.exe", "-c"]

description:
  @echo 'This file is used to run all automated commands!'

format: ruff-check-fix ruff-format mypy

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
