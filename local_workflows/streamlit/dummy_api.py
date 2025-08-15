# dummy_api_backend.py
import logging
import time
import uuid
from typing import Dict, Any, Union, List
from datetime import datetime, timezone
import requests

logger = logging.getLogger(__name__)


class DummyAPIBackend:
    """
    Local-only dummy backend that mimics APIBackend's interface and returns realistic
    Prefect-like responses without touching the network.

    Intended for end-to-end UI debugging and contract testing.
    """

    # --- construction ---------------------------------------------------------
    def __init__(self, flow_name: str):
        self.api_url = "http://dummy-prefect.local/api"
        self.api_version = self.get_api_version()

        # in-memory stores
        self._flows: Dict[str, Dict[str, Any]] = {}
        self._deployments: Dict[str, Dict[str, Any]] = {}
        self._schedules: Dict[str, Dict[str, Any]] = {}  # kept for compatibility; no longer authoritative
        self._flow_runs: Dict[str, Dict[str, Any]] = {}

        # register the controller flow, like the real backend does
        self.flow_id = self.register_controller_flow(flow_name)

    # --- helpers --------------------------------------------------------------
    @staticmethod
    def _new_id(prefix: str = "") -> str:
        return (prefix + "-" if prefix else "") + uuid.uuid4().hex[:12]

    def _now(self) -> float:
        return time.time()

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    def _require_deployment(self, deployment_id: str) -> Dict[str, Any]:
        dep = self._deployments.get(deployment_id)
        if not dep:
            raise ValueError(f"Deployment not found: {deployment_id}")
        return dep

    def _require_flow_run(self, flow_run_id: str) -> Dict[str, Any]:
        fr = self._flow_runs.get(flow_run_id)
        if not fr:
            raise ValueError(f"Flow run not found: {flow_run_id}")
        return fr

    # --- parity with real APIBackend -----------------------------------------
    def get_api_version(self) -> str:
        # Pretend we hit /admin/version and got a version string
        version = "3.0.0-dummy"
        logger.info("Detected Prefect API version (dummy): %s", version)
        return version

    def build_headers(self) -> dict:
        # Not used, but kept for parity
        return {
            "Content-Type": "application/json",
            "x-prefect-api-version": self.api_version,
        }

    # flows --------------------------------------------------------------------
    def register_controller_flow(self, flow_name: str):
        # Mimic POST /flows/ -> {'id': ...}
        existing = next((fid for fid, f in self._flows.items() if f["name"] == flow_name), None)
        if existing:
            logger.info("Flow %s already registered (dummy): %s", flow_name, existing)
            return existing
        fid = self._new_id("flow")
        self._flows[fid] = {"id": fid, "name": flow_name, "created": self._now()}
        logger.info("Registered flow (dummy): %s", flow_name)
        return fid

    # deployments --------------------------------------------------------------
    def register_deployment(self, deployment_name: str):
        # Mimic POST /deployments/ -> deployment object
        # The real code sends flow_id, path, entrypoint, work_pool_name, etc.
        did = self._new_id("dep")
        dep = {
            "id": did,
            "name": deployment_name,
            "flow_id": self.flow_id,
            "status": {"status": "READY"},  # UI code supports string OR dict
            "created": self._now(),
            "path": "/app/prefect",
            "entrypoint": "data_pipeline_flow.py:controller_driver_flow",
            "work_pool_name": "default",
            "enforce_parameter_schema": False,
            # fields your UI expects on GET:
            "paused": False,
            "schedules": [],  # list of schedule records
        }
        self._deployments[did] = dep
        logger.info("Registering deployment (dummy): %s", deployment_name)
        return dep

    def check_deployment_status(self, deployment_id: str):
        # Mimic GET /deployments/{id}
        dep = self._require_deployment(deployment_id)
        # Optionally simulate a brief NOT_READY window just after creation
        age = self._now() - dep["created"]
        if age < 1.0:
            # Return the same shape but NOT_READY status
            return {
                **dep,
                "status": {"status": "NOT_READY"},
            }
        # Return the record including paused + schedules
        return dep

    def create_deployment_schedules(
        self, deployment_id: str, payload: list[dict]
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Dummy version: simulates Prefect 3 'Create Deployment Schedules' response
        without performing a real HTTP call. Intended for TEST_MODE.

        Returns a dict with a top-level 'schedules' list, where each item looks like
        what Prefect would return (id/created/updated/schedule/deployment_id/active/parameters/...).
        """
        path = f"/deployments/{deployment_id}/schedules"
        logger.info("Creating deployment schedules (DUMMY)... path=%s", path)
        logger.debug("Payload (DUMMY): %s", payload)

        dep = self._require_deployment(deployment_id)

        # ---- basic validation mirroring expectations ----
        if not isinstance(payload, list) or not payload:
            raise ValueError("Payload must be a non-empty list of schedule objects.")

        now_iso = self._now_iso()
        created_scheds: list[Dict[str, Any]] = []

        for i, item in enumerate(payload, start=1):
            if not isinstance(item, dict):
                raise ValueError(f"Payload[{i-1}] must be a dict.")
            schedule_obj = item.get("schedule")
            if not isinstance(schedule_obj, dict):
                raise ValueError(f"Payload[{i-1}]['schedule'] must be a dict.")
            # optional fields
            active = bool(item.get("active", True))
            max_runs = item.get("max_scheduled_runs")
            params = item.get("parameters") or {}
            slug = item.get("slug") or f"dummy-{deployment_id[:8]}-{i}"

            # Simulated schedule record as Prefect would echo back
            rec = {
                "id": str(uuid.uuid4()),
                "created": now_iso,
                "updated": now_iso,
                "schedule": schedule_obj,          # echo what you sent (RRule/Cron/Interval object)
                "deployment_id": deployment_id,
                "active": active,
                "max_scheduled_runs": (
                    int(max_runs) if isinstance(max_runs, int) and max_runs > 0 else None
                ),
                "parameters": params,
                "slug": slug,
            }
            created_scheds.append(rec)

        # ---- persist on the deployment so subsequent GETs see them ----
        dep["schedules"].extend(created_scheds)

        # (legacy) keep _schedules mapping pointing to "last created" schedule for this deployment
        # to preserve compatibility with any old code that referenced it
        if created_scheds:
            self._schedules[deployment_id] = created_scheds[-1]

        response: Dict[str, Any] = {
            "id": str(uuid.uuid4()),
            "created": now_iso,
            "updated": now_iso,
            "deployment_id": deployment_id,
            "schedules": created_scheds,
        }

        logger.debug("Response (DUMMY): %s", response)
        return response

    def pause_deployment_schedule(self, deployment_id: str) -> None:
        dep = self._deployments.get(deployment_id)
        if not dep:
            msg = f"HTTP 404 /api/deployments/{deployment_id}/pause_deployment — Deployment not found"
            logger.error(msg)
            raise requests.exceptions.HTTPError(msg)

        dep["paused"] = True

        logger.info("Paused deployment (dummy): %s", deployment_id)
        return None

    def resume_deployment_schedule(self, deployment_id: str) -> None:
        dep = self._deployments.get(deployment_id)
        if not dep:
            msg = f"HTTP 404 /api/deployments/{deployment_id}/resume_deployment — Deployment not found"
            logger.error(msg)
            raise requests.exceptions.HTTPError(msg)

        dep["paused"] = False

        logger.info("Resumed deployment (dummy): %s", deployment_id)
        return None

    # flow runs ---------------------------------------------------------------
    def run_deployed_flow(
        self,
        deployment_id: str,
        provider: str,
        command_type: str,
        command: dict,
    ):
        """
        Mimic POST /deployments/{id}/create_flow_run
        Validates minimal command contents, returns a flow run object.
        """
        dep = self._require_deployment(deployment_id)

        # Minimal validation by command_type
        if command_type == "fetch_historical":
            required = ["ticker", "exchange", "interval", "start", "end"]
            missing = [k for k in required if k not in command or not command[k]]
            if missing:
                msg = f"Missing parameters for {command_type}: {', '.join(missing)}"
                logger.error(msg)
                return {"error": "BadRequest", "detail": msg}
        elif command_type == "start_stream":
            try:
                dur = int(command.get("duration", 0))
                if dur <= 0:
                    raise ValueError
            except Exception:
                msg = "Invalid 'duration' for start_stream; expected positive integer"
                logger.error(msg)
                return {"error": "BadRequest", "detail": msg}
        else:
            msg = f"Unknown command_type: {command_type}"
            logger.error(msg)
            return {"error": "BadRequest", "detail": msg}

        fr_id = self._new_id("run")
        name = f"{dep['name']}-{fr_id[-6:]}"
        created = self._now()

        # Store with a deterministic lifecycle: RUNNING for 5s, then COMPLETED
        self._flow_runs[fr_id] = {
            "id": fr_id,
            "name": name,
            "deployment_id": dep["id"],
            "parameters": {
                "provider": provider,
                "command_type": command_type,
                "command": command,
            },
            "state": {"type": "RUNNING", "name": "Running"},
            "created": created,
        }
        logger.info("Started flow run (dummy): %s", fr_id)

        # Real endpoint returns a run object; keep it close
        return {
            "id": fr_id,
            "name": name,
            "state_type": "RUNNING",
            "deployment_id": dep["id"],
        }

    def check_flow_run_status(self, flow_run_id: str):
        """
        Mimic GET /flow_runs/{id}
        Returns nested 'state' with {'type': ...} and a 'name'; UI normalizer supports both.
        """
        fr = self._require_flow_run(flow_run_id)
        age = self._now() - fr["created"]
        if age >= 5.0 and fr["state"]["type"] not in {"COMPLETED", "FAILED", "CANCELLED", "CRASHED"}:
            # deterministically complete after 5 seconds
            fr["state"] = {"type": "COMPLETED", "name": "Completed"}

        return {
            "id": fr["id"],
            "name": fr["name"],
            "deployment_id": fr["deployment_id"],
            "state": fr["state"],  # {'type': 'RUNNING'|'COMPLETED', ...}
        }

    def delete_deployment(self, deployment_id: str) -> None:
        dep = self._deployments.pop(deployment_id, None)
        if not dep:
            msg = f"HTTP 404 /api/deployments/{deployment_id} — Deployment not found"
            logger.error(msg)
            raise requests.exceptions.HTTPError(msg)

        # Remove schedules
        removed_scheds = self._schedules.pop(deployment_id, [])
        n_sched = len(removed_scheds)

        # Remove flow runs tied to this deployment
        to_delete_runs = [rid for rid, fr in self._flow_runs.items() if fr["deployment_id"] == deployment_id]
        for rid in to_delete_runs:
            self._flow_runs.pop(rid, None)
        n_runs = len(to_delete_runs)

        logger.info("Deleted deployment (dummy): %s; removed schedules=%d, flow_runs=%d",
                    deployment_id, n_sched, n_runs)
        return None
