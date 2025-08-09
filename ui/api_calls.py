import requests
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)


class APIBackend:
    def __init__(self, flow_name: str):
        self.api_url = "http://prefect-server:4200/api"
        self.api_version = self.get_api_version()
        self.flow_id = self.register_controller_flow(flow_name)

    @lru_cache(maxsize=1) # This just caches the result of this function so it doesn't call the API every time
    def get_api_version(self) -> str:
        """
        Fetches the Prefect Server API version via the Read Version endpoint,
        caches it so we only call it once per process.
        """
        try:
            url = f"{self.api_url}/admin/version"
            resp = requests.get(url)
            resp.raise_for_status()
            # The server returns a bare JSON string, e.g. "3.0.0"
            version = resp.json()
            logger.info("Detected Prefect API version: %s", version)
            return version
        except requests.exceptions.RequestException as e:
            logger.error("HTTP request failed: %s", str(e), exc_info=True)
            raise

    def build_headers(self) -> dict:
        """
        Builds the headers for Prefect API calls, injecting the correct version.
        """
        return {
            "Content-Type": "application/json",
            "x-prefect-api-version": self.api_version
        }

    def send(self, url, payload, headers, method="POST", timeout=30):
        try:
            if method == "POST":
                response = requests.post(url, json=payload, headers=headers, timeout=timeout)
            elif method == "DELETE":
                response = requests.delete(url, headers=headers, timeout=timeout)
            else:
                raise ValueError(f"Unsupported method: {method}")

            response.raise_for_status()
            if response.status_code == 204 or not response.content:
                logger.info("API request successful (no content).")
                return {"deleted": True}

            logger.info("API request successful.")
            logger.debug("Response: %s", response.text)
            return response.json()

        except requests.exceptions.HTTPError as e:
            if e.response is not None:
                try:
                    logger.error("HTTP %s response: %s", e.response.status_code, e.response.json())
                except Exception:
                    logger.error("HTTP %s response (non-JSON): %s", e.response.status_code, e.response.text)
            raise
        except requests.exceptions.RequestException as e:
            logger.error("HTTP request failed: %s", str(e), exc_info=True)
            raise

    def check_deployment_status(self, deployment_id: str):
        try:
            url = f"{self.api_url}/deployments/{deployment_id}"
            headers = self.build_headers()
            logger.info(f"Checking deployment status...")
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            logger.info("Deployment status retrieved successfully.")
            logger.debug("Response: %s", response.text)
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error("HTTP request failed: %s", str(e), exc_info=True)
            raise

    def check_flow_run_status(self, flow_run_id: str):
        try:
            url = f"{self.api_url}/flow_runs/{flow_run_id}"
            headers = self.build_headers()
            logger.info(f"Checking status for flow run {flow_run_id}...")
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            logger.info("Flow run status retrieved successfully.")
            logger.debug("Response: %s", response.text)
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error("HTTP request failed: %s", str(e), exc_info=True)
            raise

    def register_controller_flow(self, flow_name):
        """
        Creates (or retrieves) the specified flow name on Prefect Server
        and returns its unique flow ID.
        """
        url = f"{self.api_url}/flows/"
        payload = {"name": flow_name}
        headers = self.build_headers()

        logger.info(f"Registering flow...")
        logger.debug("POST %s", url)
        logger.debug("Payload: %s", payload)

        response = self.send(url, payload, headers)
        flow_id = response['id']
        return flow_id

    def register_deployment(self, deployment_name: str,
                            schedules: list[dict] | None = None):
        schedules = schedules or []

        url = f"{self.api_url}/deployments/"
        payload = {
            "name": deployment_name,
            "flow_id": self.flow_id,
            "path": "/app/prefect",
            "entrypoint": "data_pipeline_flow.py:controller_driver_flow",
            "work_pool_name": "default",
            "schedules": schedules,
            "enforce_parameter_schema": False,
        }
        headers = self.build_headers()

        logger.info("Registering deployment: %s", deployment_name)
        logger.debug("POST %s", url)
        logger.debug("Payload: %s", payload)

        response = self.send(url, payload, headers)
        return response

    def run_deployed_flow(self, deployment_id: str, provider: str, command_type: str,
                          command: dict):
        url = f"{self.api_url}/deployments/{deployment_id}/create_flow_run"
        payload = {
            "parameters": {
                "command": command,
                "command_type": command_type,
                "provider": provider
            }
        }
        headers = self.build_headers()

        logger.info("Running flow from deployment...")
        logger.debug("POST %s", url)
        logger.debug("Payload: %s", payload)

        response = self.send(url, payload, headers)
        return response

    def delete_deployment(self, deployment_id: str):
        url = f"{self.api_url}/deployments/{deployment_id}"
        headers = self.build_headers()

        logger.info("Deleting deployment: %s", deployment_id)
        logger.debug("DELETE %s", url)

        response = self.send(url, None, headers, method='DELETE')
        return response
