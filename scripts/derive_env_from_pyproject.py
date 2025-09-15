#!/usr/bin/env python

ENV_PATH = ".env"

def get_python_version_truncated():
    with open(".python-version", "r") as f:
        full_version = f.read().strip()
    return ".".join(full_version.split(".")[:2])  # e.g., "3.13.5" → "3.13"

def get_prefect_version(pyproject_path: str = "pyproject.toml"):
    import tomllib
    with open(pyproject_path, "rb") as f:
        config = tomllib.load(f)
    for dep in config["project"]["dependencies"]:
        if dep.startswith("prefect"):
            parts = dep.split("==")
            if len(parts) == 2:
                return parts[1]
    return None

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
    python_version = get_python_version_truncated()
    prefect_version = get_prefect_version()
    update_env_file(python_version, prefect_version)
