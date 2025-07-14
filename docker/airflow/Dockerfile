# syntax=docker/dockerfile:1.4

ARG PYTHON_VERSION
ARG AIRFLOW_VERSION
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

USER root
COPY dags /opt/airflow/dags
