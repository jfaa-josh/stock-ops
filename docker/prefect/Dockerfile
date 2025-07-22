# syntax=docker/dockerfile:1.4

ARG PYTHON_VERSION
ARG PREFECT_VERSION
FROM prefecthq/prefect:${PREFECT_VERSION}-python${PYTHON_VERSION}

WORKDIR /app

COPY flows/ /app/flows

CMD ["python", "flows/data_pipeline_flow.py"]
