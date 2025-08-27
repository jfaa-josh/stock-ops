import requests
import logging
from functools import lru_cache
from typing import Dict, Any, List, Union
import os

import api_client

logger = logging.getLogger(__name__)


class APIBackend:
    def __init__(self, flow_name: str):
        self.api_url = "http://prefect-server:4200/api"
        self.api_version = self.get_api_version()
        self.api_client = api_client.ApiClient(base_url = self.api_url,
                                               default_headers = self.build_headers(),)
        self.verbose_logging = os.getenv("VERBOSE_LOGGING", "false").lower() == "true"
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

    def check_deployment_status(self, deployment_id: str):
        try:
            path = f"/deployments/{deployment_id}"
            logger.info(f"Checking deployment status...")
            response = self.api_client.send(path, method = "GET")
            if self.verbose_logging: logger.debug("Response: %s", response)
            return response
        except requests.exceptions.RequestException as e:
            logger.error("HTTP request failed: %s", str(e), exc_info=True)
            raise

    def create_deployment_schedules(self, deployment_id: str, payload: list[dict]) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        path = f"/deployments/{deployment_id}/schedules"
        logger.info("Creating deployment schedules...")
        response = self.api_client.send(path, payload=payload, method="POST")
        logger.debug("Payload: %s", payload)
        if self.verbose_logging: logger.debug("Response: %s", response)
        return response

    def check_flow_run_status(self, flow_run_id: str):
        try:
            path = f"/flow_runs/{flow_run_id}"
            logger.info(f"Checking status for flow run {flow_run_id}...")
            response = self.api_client.send(path, method = "GET")
            if self.verbose_logging: logger.debug("Response: %s", response)
            return response
        except requests.exceptions.RequestException as e:
            logger.error("HTTP request failed: %s", str(e), exc_info=True)
            raise

    def register_controller_flow(self, flow_name):
        try:
            path = "/flows/"
            payload = {"name": flow_name}
            logger.info(f"Registering flow {flow_name}")
            response = self.api_client.send(path, payload = payload, method = "POST")
            logger.debug("Payload: %s", payload)
            if self.verbose_logging: logger.debug("Response: %s", response)
            return response['id']
        except requests.exceptions.RequestException as e:
            logger.error("HTTP request failed: %s", str(e), exc_info=True)
            raise

    def register_deployment(self, deployment_name: str):
        try:
            path = "/deployments/"
            payload = {
                "name": deployment_name,
                "flow_id": self.flow_id,
                "path": "/app/prefect",
                "entrypoint": "data_pipeline_flow.py:controller_driver_flow",
                "work_pool_name": "default",
                "enforce_parameter_schema": False,
            }
            logger.info("Registering deployment: %s", deployment_name)
            response = self.api_client.send(path, payload = payload,
                                            method = "POST")
            logger.debug("Payload: %s", payload)
            if self.verbose_logging: logger.debug("Response: %s", response)
            return response
        except requests.exceptions.RequestException as e:
            logger.error("HTTP request failed: %s", str(e), exc_info=True)
            raise

    def run_deployed_flow(self, deployment_id: str, provider: str, command_type: str,
                          command: dict):
        try:
            path = f"/deployments/{deployment_id}/create_flow_run"
            payload = {
                "parameters": {
                    "command": command,
                    "command_type": command_type,
                    "provider": provider
                }
            }
            logger.info("Running flow from deployment...")
            response = self.api_client.send(path, payload = payload,
                                            method = "POST")
            logger.debug("Payload: %s", payload)
            if self.verbose_logging: logger.debug("Response: %s", response)
            return response
        except requests.exceptions.RequestException as e:
            logger.error("HTTP request failed: %s", str(e), exc_info=True)
            raise

    def delete_deployment(self, deployment_id: str) -> None:
        try:
            path = f"/deployments/{deployment_id}"
            logger.info("Deleting deployment: %s", deployment_id)
            _ = self.api_client.send(path, method = "DELETE")
            return None
        except requests.exceptions.RequestException as e:
            logger.error("HTTP request failed: %s", str(e), exc_info=True)
            raise

    def pause_deployment_schedule(self, deployment_id: str) -> None:
        try:
            path = f"/deployments/{deployment_id}/pause_deployment"
            logger.info("Pausing deployment schedule...")
            _ = self.api_client.send(path, method = "POST")
            return None
        except requests.exceptions.RequestException as e:
            logger.error("HTTP request failed: %s", str(e), exc_info=True)
            raise

    def resume_deployment_schedule(self, deployment_id: str) -> None:
        try:
            path = f"/deployments/{deployment_id}/resume_deployment"
            logger.info("Resuming deployment schedule...")
            _ = self.api_client.send(path, method = "POST")
            return None
        except requests.exceptions.RequestException as e:
            logger.error("HTTP request failed: %s", str(e), exc_info=True)
            raise
