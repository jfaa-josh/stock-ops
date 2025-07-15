# syntax=docker/dockerfile:1.4

ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}-slim

WORKDIR /app
COPY . /app

RUN pip install -e .

CMD ["uvicorn", "stockops.runtime.data_pipeline_api:app", "--host", "0.0.0.0", "--port", "8000"]
