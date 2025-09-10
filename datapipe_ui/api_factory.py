from typing import Protocol, runtime_checkable, Any, Dict, Union, List
from pathlib import Path
from contextlib import contextmanager
import sys

import api_backend


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
            from local_workflows.streamlit.dummy_api import DummyAPIBackend
        return DummyAPIBackend(flow)
    return api_backend.APIBackend(flow)

@runtime_checkable          # lets you do isinstance(obj, ApiLike) at runtime
class ApiLike(Protocol):
    # deployments
    def register_deployment(self, deployment_name: str) -> Dict[str, Any]: ...
    def check_deployment_status(self, deployment_id: str) -> Dict[str, Any]: ...
    def delete_deployment(self, deployment_id: str) -> None: ...

    # schedules
    def create_deployment_schedules(self, deployment_id: str, payload: list[dict]) -> Union[Dict[str, Any], List[Dict[str, Any]]]: ...
    def pause_deployment_schedule(self, deployment_id: str) -> None: ...
    def resume_deployment_schedule(self, deployment_id: str) -> None: ...

    # flow runs
    def run_deployed_flow(
        self, deployment_id: str, provider: str, command_type: str, command: Dict[str, Any]
    ) -> Dict[str, Any]: ...
    def check_flow_run_status(self, flow_run_id: str) -> Dict[str, Any]: ...
