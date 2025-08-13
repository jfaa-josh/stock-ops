from typing import Any, Tuple
from api_factory import ApiLike

class DeploymentService:
    def __init__(self, api: ApiLike, provider: str = "EODHD"):
        self.api = api
        self.provider = provider

    def create_deployment(self, cfg: dict[str, Any]) -> bool:
        if cfg.get("deployment_id") is None:
            resp = self.api.register_deployment(cfg["deployment_name"], schedules=cfg.get("schedules") or None)
            cfg["deployment_id"] = resp["id"]
        dep = self.api.check_deployment_status(cfg["deployment_id"])
        s = dep.get("status")
        status_str = s.get("status") if isinstance(s, dict) else s
        return status_str == "READY"

    def run_historical(self, cfg: dict[str, Any]) -> Tuple[str, str]:
        command = {
            "ticker": cfg["ticker"], "exchange": cfg["exchange"],
            "interval": cfg["interval"], "start": cfg["start"], "end": cfg["end"],
        }
        resp = self.api.run_deployed_flow(cfg["deployment_id"], self.provider, "fetch_historical", command)
        return resp["id"], resp["name"]

    def run_stream(self, cfg: dict[str, Any]) -> Tuple[str, str]:
        command = {
            "tickers": cfg["ticker"], "exchange": cfg["exchange"],
            "stream_type": cfg["stream_type"], "duration": int(cfg["duration"]),
        }
        resp = self.api.run_deployed_flow(cfg["deployment_id"], self.provider, "start_stream", command)
        return resp["id"], resp["name"]

    def get_exchange_tz(self, exchange: str) -> str:
        try:
            return self.api.get_tz(self.provider, exchange)
        except Exception:
            return "UTC"
