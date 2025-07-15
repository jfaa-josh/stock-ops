#!/usr/bin/env python
import tomllib

ENV_PATH = ".env"

def extract_versions(pyproject_path: str = "pyproject.toml"):
    with open(pyproject_path, "rb") as f:
        config = tomllib.load(f)

    python_spec = config["project"]["requires-python"]
    full_python_version = python_spec.strip("><=~^").split(",")[0]

    prefect_version = None
    for dep in config["project"]["dependencies"]:
        if dep.startswith("prefect"):
            parts = dep.split("==")
            if len(parts) == 2:
                prefect_version = parts[1]
            break

    return full_python_version, prefect_version

def update_env_file(python_version: str, prefect_version: str | None):
    try:
        with open(ENV_PATH, "r") as f:
            lines = f.readlines()
    except FileNotFoundError:
        lines = []

    new_lines = []
    found_keys = set()

    for line in lines:
        if "=" not in line or line.strip().startswith("#"):
            new_lines.append(line)
            continue

        key, _, _ = line.partition("=")
        key = key.strip()

        if key == "PYTHON_VERSION":
            new_lines.append(f"PYTHON_VERSION={python_version}\n")
            found_keys.add(key)
        elif key == "PREFECT_VERSION" and prefect_version:
            new_lines.append(f"PREFECT_VERSION={prefect_version}\n")
            found_keys.add(key)
        else:
            new_lines.append(line)

    if "PYTHON_VERSION" not in found_keys:
        new_lines.append(f"PYTHON_VERSION={python_version}\n")
    if prefect_version and "PREFECT_VERSION" not in found_keys:
        new_lines.append(f"PREFECT_VERSION={prefect_version}\n")

    with open(ENV_PATH, "w") as f:
        f.writelines(new_lines)

if __name__ == "__main__":
    python_version, prefect_version = extract_versions()
    update_env_file(python_version, prefect_version)
