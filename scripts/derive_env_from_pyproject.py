#!/usr/bin/env python
import tomllib

ENV_PATH = ".env"
KEYS_TO_UPDATE = {"PYTHON_VERSION", "AIRFLOW_VERSION"}

# Step 1: Parse pyproject.toml
with open("pyproject.toml", "rb") as f:
    config = tomllib.load(f)

python_spec = config["project"]["requires-python"]
python_version = python_spec.strip("><=~^").split(",")[0]
python_version = ".".join(python_version.split(".")[:2])

airflow_version = None
for dep in config["project"]["dependencies"]:
    if dep.startswith("apache-airflow"):
        parts = dep.split("==")
        if len(parts) == 2:
            airflow_version = parts[1]
        break

# Step 2: Read existing .env lines
try:
    with open(ENV_PATH, "r") as f:
        lines = f.readlines()
except FileNotFoundError:
    lines = []

# Step 3: Build updated lines
new_lines = []
found_keys = set()

for line in lines:
    if "=" not in line or line.strip().startswith("#"):
        new_lines.append(line)
        continue

    key, _, value = line.partition("=")
    key = key.strip()

    if key == "PYTHON_VERSION":
        new_lines.append(f"PYTHON_VERSION={python_version}\n")
        found_keys.add("PYTHON_VERSION")
    elif key == "AIRFLOW_VERSION" and airflow_version:
        new_lines.append(f"AIRFLOW_VERSION={airflow_version}\n")
        found_keys.add("AIRFLOW_VERSION")
    else:
        new_lines.append(line)

# Step 4: Append missing keys
if "PYTHON_VERSION" not in found_keys:
    new_lines.append(f"PYTHON_VERSION={python_version}\n")
if airflow_version and "AIRFLOW_VERSION" not in found_keys:
    new_lines.append(f"AIRFLOW_VERSION={airflow_version}\n")

# Step 5: Write back to .env
with open(ENV_PATH, "w") as f:
    f.writelines(new_lines)
