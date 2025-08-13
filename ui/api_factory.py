from typing import Protocol, runtime_checkable
from datetime import datetime, date
from typing import Any, Iterable, Optional
from pathlib import Path
from contextlib import contextmanager
import sys

import api_calls


@contextmanager
def _temp_sys_path(p: Path):
    s = str(p)
    sys.path.insert(0, s)
    try:
        yield
    finally:
        try:
            sys.path.remove(s)
        except ValueError:
            pass

def make_api(flow: str, test: bool):
    if test:
        project_root = Path(__file__).resolve().parents[1]  # contains both ui/ and local_workflows/
        with _temp_sys_path(project_root):
            from local_workflows.streamlit.dummy_api import DummyAPI
        return DummyAPI()
    return api_calls.APIBackend(flow)

@runtime_checkable          # lets you do isinstance(obj, ApiLike) at runtime
class ApiLike(Protocol):
    def register_deployment(
        self,
        deployment_name: str,
        schedules: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]: ...
    def check_deployment_status(self, deployment_id: str) -> dict[str, Any]: ...
    def delete_deployment(self, deployment_id: str) -> dict[str, Any]: ...
    def run_deployed_flow(
        self, deployment_id: str, provider: str, command_type: str, command: dict[str, Any]
    ) -> dict[str, Any]: ...
    def check_flow_run_status(self, flow_run_id: str) -> dict[str, Any]: ...
    def build_schedule(
            self,
            *,
            timezone: str,
            freq: str,
            dtstart_local: datetime,
            interval: int = 1,
            byweekday: Optional[Iterable[str]] = None,
            bymonth: Optional[Iterable[int]] = None,
            bymonthday: Optional[Iterable[int]] = None,
            bysetpos: Optional[Iterable[int]] = None,
            until_local: Optional[datetime | date] = None,
            byhour: Optional[int] = None,
            byminute: Optional[int] = None,
            bysecond: Optional[int] = None,
            active: bool = True,
        ) -> dict[str, Any]: ...

    def get_tz(self, provider: str, exchange: str = "US") -> str: ...
