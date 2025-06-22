# Use the specified Python version (with slim base image)
ARG PYTHON_VERSION=3.13.5
FROM python:${PYTHON_VERSION}-slim

# Set the working directory inside the container
WORKDIR /app

# Install uv (your package/dependency manager)
RUN pip install --no-cache-dir uv

# Copy dependency files
COPY pyproject.toml .
COPY .python-version .
COPY scripts/install.sh ./scripts/install.sh
RUN chmod +x ./scripts/install.sh && ./scripts/install.sh

# Create virtual environment and sync all dependencies (main only, for prod)
RUN uv venv .venv && \
    uv sync --strict

# Copy the rest of the application
COPY src/ src/
COPY data/ data/
COPY tests/ tests/
COPY docs/ docs/

# Activate the virtual environment as default
ENV VIRTUAL_ENV="/app/.venv"
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Optional: set entrypoint or CMD for runtime
# CMD ["python", "-m", "stockops"]
