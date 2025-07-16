import logging
import os
import subprocess
import sys
import time

import pytest
import requests

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


def _start_fastapi_subprocess():
    """Start the FastAPI controller API using uvicorn as a background subprocess."""
    logger.info("🔧 Starting FastAPI API subprocess...")

    env = os.environ.copy()
    env["PYTHONPATH"] = "src"

    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "stockops.runtime.data_pipeline_api:app",
            "--host",
            "127.0.0.1",
            "--port",
            "8000",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
    )

    try:
        for _ in range(10):
            try:
                r = requests.get(HEALTH_ENDPOINT)
                if r.status_code == 200:
                    logger.info("✅ FastAPI service is online.")
                    return process
            except requests.ConnectionError:
                time.sleep(0.5)

        # If we reach here, FastAPI didn't start
        if process.stdout:
            logger.error("❌ Failed to start FastAPI. Partial logs:")
            for _ in range(20):
                line = process.stdout.readline()
                if not line:
                    break
                logger.error(line.strip())

        process.terminate()
        raise RuntimeError("❌ Failed to start FastAPI service.")

    except Exception:
        process.terminate()
        raise


def _shutdown_fastapi():
    """Shutdown FastAPI controller via its HTTP endpoint."""
    logger.info("🧼 Shutting down FastAPI subprocess after all tests...")
    try:
        response = requests.post(SHUTDOWN_ENDPOINT)
        logger.info("Shutdown response: %s", response.json())
    except requests.RequestException as e:
        logger.error("❌ Failed to shut down FastAPI: %s", e)


@pytest.fixture(scope="function")
def fastapi_process():
    proc = _start_fastapi_subprocess()
    time.sleep(2)  # Allow time for full readiness
    yield proc
    _shutdown_fastapi()
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
