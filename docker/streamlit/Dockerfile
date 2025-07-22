# syntax=docker/dockerfile:1.4

ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}-slim

WORKDIR /app

COPY . .

ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=false \
    PYTHONUNBUFFERED=1 \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_ENABLECORS=false

EXPOSE 8501

# Switch to non-root user for security (optional)
# RUN useradd -m appuser
# USER appuser

CMD ["streamlit", "run", "streamlit_app.py"]
