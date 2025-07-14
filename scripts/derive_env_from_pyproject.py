#!/usr/bin/env python
import tomllib

with open("pyproject.toml", "rb") as f:
    config = tomllib.load(f)

python_spec = config["project"]["requires-python"]
python_full = python_spec.strip("><=~^").split(",")[0]  # e.g., "3.10"
python_minor = ".".join(python_full.split(".")[:2])

# Optional: read airflow version from dependencies
airflow_version = None
for dep in config["tool"]["uv"]["dependencies"]:
    if dep.startswith("apache-airflow"):
        airflow_version = dep.split("==")[1]
        break

with open(".env", "w") as f:
    f.write(f"PYTHON_FULL={python_full}\n")
    f.write(f"PYTHON_MINOR={python_minor}\n")
    if airflow_version:
        f.write(f"AIRFLOW_VERSION={airflow_version}\n")
