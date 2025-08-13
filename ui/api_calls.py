import requests
import logging
from functools import lru_cache
from zoneinfo import ZoneInfo
from typing import Iterable, Optional, Any
from datetime import datetime, date, time as dtime
import json

from stockops.config import utils as cfg_utils  # Add additional providers to config utils as needed

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

    def get_tz(self, provider: str, exchange: str = "US") -> str:
        cfg = cfg_utils.ProviderConfig(provider, exchange)
        return cfg.tz_str

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

            if response.status_code == 204:
                logger.info("API %s %s -> 204 (no content)", method, url)
                return {"deleted": (method == "DELETE")}

            logger.info("API %s %s -> %s", method, url, response.status_code)
            try:
                return response.json()
            except ValueError:
                # Non-JSON success (rare)
                logger.debug("Non-JSON success body: %r", response.text[:1000])
                return {"status": response.status_code, "text": response.text}

        except requests.exceptions.HTTPError as e:
            resp = e.response
            status = getattr(resp, "status_code", "<?>")
            # Try JSON first—Prefect returns clear 'detail' here
            detail = None
            body = None
            if resp is not None:
                try:
                    body = resp.json()
                    detail = body.get("detail")
                except Exception:
                    body = (resp.text or "")[:2000]

            logger.error(
                "HTTP %s on %s %s\npayload=%r\nresponse=%s",
                status, method, url, payload,
                body if isinstance(body, str) else json.dumps(body, indent=2)
            )

            # Re-raise with a concise message Streamlit will show inline
            msg = f"HTTP {status} {url}"
            if detail:
                msg += f" — {detail}"
            raise requests.exceptions.HTTPError(msg, response=resp) from None

        except requests.exceptions.RequestException as e:
            logger.error("Request error on %s %s: %s", method, url, e)
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

    def build_schedule(
        self,
        *,
        timezone: str,
        freq: str,                           # MINUTELY | HOURLY | DAILY | WEEKLY | MONTHLY | YEARLY
        dtstart_local: datetime,
        interval: int = 1,
        byweekday: Optional[Iterable[str]] = None,   # e.g. ["MO","TU","WE"]
        bymonth: Optional[Iterable[int]] = None,
        bymonthday: Optional[Iterable[int]] = None,
        bysetpos: Optional[Iterable[int]] = None,
        until_local: Optional[datetime | date] = None,
        byhour: Optional[int] = None,
        byminute: Optional[int] = None,
        bysecond: Optional[int] = None,
        active: bool = True,
    ) -> dict:
        valid_freq = {"MINUTELY", "HOURLY", "DAILY", "WEEKLY", "MONTHLY", "YEARLY"}
        if freq not in valid_freq:
            raise ValueError(f"Invalid FREQ: {freq}")
        if interval <= 0:
            raise ValueError("INTERVAL must be a positive integer")

        tz = ZoneInfo(timezone)
        # localize/normalize DTSTART
        if dtstart_local.tzinfo is None:
            dtstart_aware = dtstart_local.replace(tzinfo=tz)
        else:
            dtstart_aware = dtstart_local.astimezone(tz)

        # Default time parts from DTSTART
        h = dtstart_aware.hour if byhour is None else int(byhour)
        m = dtstart_aware.minute if byminute is None else int(byminute)
        s = dtstart_aware.second if bysecond is None else int(bysecond)

        parts = [f"FREQ={freq}", f"INTERVAL={interval}"]

        if byweekday:
            wd = [w.strip().upper() for w in byweekday]
            allowed = {"MO", "TU", "WE", "TH", "FR", "SA", "SU"}
            if not set(wd).issubset(allowed):
                raise ValueError(f"Invalid BYDAY tokens: {byweekday}")
            parts.append(f"BYDAY={','.join(wd)}")

        def _join_ints(name: str, values: Optional[Iterable[int]], lo: int, hi: int):
            if values is None:
                return
            vals = list(values)
            for v in vals:
                if v < lo or v > hi:
                    raise ValueError(f"{name} value {v} out of range [{lo},{hi}]")
            parts.append(f"{name}=" + ",".join(str(v) for v in vals))

        _join_ints("BYMONTH", bymonth, 1, 12)
        _join_ints("BYMONTHDAY", bymonthday, -31, 31)
        _join_ints("BYSETPOS", bysetpos, -366, 366)

        # Emit BY* fields conditionally to avoid restricting HOURLY/MINUTELY too much
        if freq in {"DAILY", "WEEKLY", "MONTHLY", "YEARLY"}:
            parts.append(f"BYHOUR={h}")
            parts.append(f"BYMINUTE={m}")
            parts.append(f"BYSECOND={s}")
        elif freq == "HOURLY":
            parts.append(f"BYMINUTE={m}")
            parts.append(f"BYSECOND={s}")
        elif freq == "MINUTELY":
            parts.append(f"BYSECOND={s}")

        # UNTIL handling (RFC 5545): if datetime -> convert to UTC and suffix Z; if date -> end-of-day local then convert
        if until_local is not None:
            if isinstance(until_local, date) and not isinstance(until_local, datetime):
                until_dt = datetime.combine(until_local, dtime(23, 59, 59))
            else:
                until_dt = until_local  # type: ignore[assignment]
            # localize to tz
            if until_dt.tzinfo is None:
                until_localized = until_dt.replace(tzinfo=tz)
            else:
                until_localized = until_dt.astimezone(tz)

                until_utc = until_localized.astimezone(ZoneInfo("UTC"))
                parts.append("UNTIL=" + until_utc.strftime("%Y%m%dT%H%M%SZ"))

            # simple sanity check: UNTIL must be after DTSTART
            if until_localized <= dtstart_aware:
                raise ValueError("UNTIL must be after DTSTART in local exchange time")

            until_utc = until_localized.astimezone(ZoneInfo("UTC"))
            parts.append("UNTIL=" + until_utc.strftime("%Y%m%dT%H%M%SZ"))

        rrule_value = ";".join(parts)

        result: dict[str, Any] = {
            "active": bool(active),
            "schedule": {
                "rrule": rrule_value,
                "timezone": timezone,
                # IMPORTANT: do NOT include 'dtstart' – Prefect 3 treats it as extra for RRuleSchedule
            },
        }

        return result

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
