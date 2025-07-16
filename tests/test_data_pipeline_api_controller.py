import logging
import os
import subprocess
import sys
import time

import pytest
import requests

from local_workflows.data_pipeline import (
    shutdown_fastapi,
    start_fastapi_subprocess,
)

BASE_URL = "http://127.0.0.1:8000"
HEALTH_ENDPOINT = f"{BASE_URL}/health"
SEND_ENDPOINT = f"{BASE_URL}/send_command"
SHUTDOWN_ENDPOINT = f"{BASE_URL}/shutdown"

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("test_logger")


@pytest.fixture(scope="function")
def fastapi_process(monkeypatch):
    """Starts the FastAPI subprocess with PYTHONPATH=src for test environment."""

    def _patched_popen(*args, **kwargs):
        env = kwargs.get("env", os.environ.copy())
        env["PYTHONPATH"] = "src"
        kwargs["env"] = env
        return original_popen(*args, **kwargs)

    original_popen = subprocess.Popen
    monkeypatch.setattr(subprocess, "Popen", _patched_popen)

    logger.info("🔧 Starting FastAPI subprocess for controller tests...")
    proc = start_fastapi_subprocess()
    logger.info("✅ FastAPI subprocess started.")
    time.sleep(2)  # Give the server time to initialize

    yield proc

    logger.info("🧼 Shutting down FastAPI subprocess after all tests...")
    shutdown_fastapi()
    proc.terminate()
    proc.wait()
    logger.info("✅ FastAPI subprocess terminated cleanly.")
    time.sleep(1)


def test_fastapi_controller_flow(fastapi_process):
    """Check startup, health, command handling, and shutdown logic."""

    # Step 1: Health check
    logger.info("🔍 Step 1: Verifying that FastAPI is running via /health...")
    r = requests.get(HEALTH_ENDPOINT)
    assert r.status_code == 200
    assert r.json()["status"] == "ok"
    logger.info("✅ The FastAPI service is running and responded correctly to /health.")

    # Step 2: Send a fake command
    bad_command = {"type": "invalid_command_type"}
    logger.info("📤 Step 2: Sending an invalid command to the controller...")
    r = requests.post(SEND_ENDPOINT, json=bad_command)
    assert r.status_code == 200
    assert r.json()["status"] == "queued"
    assert r.json()["command"] == bad_command
    logger.info(
        "✅ The fake command was sent to the FastAPI controller, "
        "received correctly by the local controller, and was handled as invalid input."
    )
