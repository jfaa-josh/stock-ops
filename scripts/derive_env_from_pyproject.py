#!/usr/bin/env python
import tomllib

with open("pyproject.toml", "rb") as f:
    config = tomllib.load(f)

# Extract python version
python_spec = config["project"]["requires-python"]
python_version = python_spec.strip("><=~^").split(",")[0]
python_version = ".".join(python_version.split(".")[:2])  # Drop patch version if present

# Look for apache-airflow in core dependencies
airflow_version = None
for dep in config["project"]["dependencies"]:
    if dep.startswith("apache-airflow"):
        parts = dep.split("==")
        if len(parts) == 2:
            airflow_version = parts[1]
        break

# Write to .env
with open(".env", "w") as f:
    f.write(f"PYTHON_VERSION={python_version}\n")
    if airflow_version:
        f.write(f"AIRFLOW_VERSION={airflow_version}\n")
