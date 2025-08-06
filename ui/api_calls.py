import requests
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)

API_URL = "http://prefect-server:4200/api"

@lru_cache(maxsize=1) # This just caches the result of this function so it doesn't call the API every time
def get_api_version() -> str:
    """
    Fetches the Prefect Server API version via the Read Version endpoint,
    caches it so we only call it once per process.
    """
    url = f"{API_URL}/admin/version"
    resp = requests.get(url)
    resp.raise_for_status()
    # The server returns a bare JSON string, e.g. "3.0.0"
    version = resp.json()
    logger.info("Detected Prefect API version: %s", version)
    return version

def build_headers() -> dict:
    """
    Builds the headers for Prefect API calls, injecting the correct version.
    """
    return {
        "Content-Type": "application/json",
        "x-prefect-api-version": get_api_version()
    }

def send(url, payload, headers):
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        logger.info("API send successfull.")
        logger.debug("Response: %s", response.text)
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error("HTTP request failed: %s", str(e), exc_info=True)
        raise

def register_controller_flow(flow_name):
    """
    Creates (or retrieves) the specified flow name on Prefect Server
    and returns its unique flow ID.
    """
    url = f"{API_URL}/flows/"
    payload = {"name": flow_name}
    headers = build_headers()

    logger.info(f"Registering flow...")
    logger.debug("POST %s", url)
    logger.debug("Payload: %s", payload)

    response = send(url, payload, headers)
    return response

def register_deployment(flow_id, deployment_name):
    url = f"{API_URL}/deployments/"
    payload = {
        "name": deployment_name,
        "flow_id": flow_id,
        "entrypoint": "/app/prefect/data_pipeline_flow.py:controller_driver_flow", # !!! THIS NEEDS TO PROBABLY COME FROM CLASS DEF
        "path": "prefect/data_pipeline_flow.py",
        "work_pool_name": "default",
        # No schedules = on‐demand runs only; add a list of DeploymentScheduleCreate
        "schedules": [],
    }
    headers = build_headers()

    logger.info("Registering deployment:", deployment_name)
    logger.debug("POST %s", url)
    logger.debug("Payload: %s", payload)

    response = send(url, payload, headers)
    return response

# def trigger_flow(command_type, command_payload, provider="EODHD"):
#     headers = {"Content-Type": "application/json"}
#     url = f"{API_URL}/flow_runs/"
#     payload = {
#         "flow_name": "controller_driver_flow",
#         "name": "manual-trigger",
#         "parameters": {
#             "command_type": command_type,
#             "command": command_payload,
#             "provider": provider
#         }
#     }

#     logger.info(f"Triggering flow run: {command_type}")
#     logger.debug("POST %s", url)
#     logger.debug("Payload: %s", payload)

#     response = send(url, payload, headers)
#     return response
