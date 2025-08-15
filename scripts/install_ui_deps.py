#!/usr/bin/env python3
import tomllib, subprocess, sys

# 1. load your pyproject.toml
with open("pyproject.toml", "rb") as f:
    cfg = tomllib.load(f)

# 2. grab the ui extra list
ui_deps = cfg["project"]["optional-dependencies"]["ui"]
if not ui_deps:
    sys.exit(0)

# 3. install via pip
subprocess.check_call([sys.executable, "-m", "pip", "install"] + ui_deps)
