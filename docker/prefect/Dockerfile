# syntax=docker/dockerfile:1.4

ARG PYTHON_VERSION
ARG PREFECT_VERSION
FROM prefecthq/prefect:${PREFECT_VERSION}-python${PYTHON_VERSION}

WORKDIR /app

COPY pyproject.toml /app/
RUN pip install --no-cache-dir .

COPY . /app
