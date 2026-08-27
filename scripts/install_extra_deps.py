#!/usr/bin/env python3
import sys
import subprocess
import tomllib

group = sys.argv[1]

with open("pyproject.toml", "rb") as f:
    cfg = tomllib.load(f)

deps = cfg["project"]["optional-dependencies"][group]
if not deps:
    sys.exit(0)

subprocess.check_call([sys.executable, "-m", "pip", "install"] + deps)
