import importlib
import sys
from contextlib import contextmanager
from pathlib import Path

import pytest

ENTRYPOINTS = [
    ("local_workflows.streamlit_local.local_streamlit_ci", "main"),
    ("local_workflows.local_ETL", "test_ci"),
    ("local_workflows.local_write", "main"),
    ("local_workflows.reader_local.local_read_ci", "main"),
]

pytestmark = pytest.mark.integration


@pytest.fixture(scope="session")
def project_root() -> Path:
    """Repo root: adjust if your test layout differs."""
    return Path(__file__).resolve().parents[1]


@contextmanager
def _sys_path(path: Path):
    """Temporarily prepend a path to sys.path."""
    p = str(path)
    inserted = False
    if p not in sys.path:
        sys.path.insert(0, p)
        inserted = True
    try:
        yield
    finally:
        if inserted and sys.path and sys.path[0] == p:
            sys.path.pop(0)
        else:
            # Best-effort removal if position changed
            try:
                sys.path.remove(p)
            except ValueError:
                pass


@pytest.mark.timeout(150, method="thread")
@pytest.mark.parametrize("modpath, funcname", ENTRYPOINTS, ids=lambda x: x if isinstance(x, str) else x[0])
def test_entrypoints_run_clean(project_root: Path, modpath, funcname):
    with _sys_path(project_root):
        mod = importlib.import_module(modpath)
        ret = getattr(mod, funcname)()
        exit_code = int(ret) if isinstance(ret, int) else 0
        assert exit_code == 0
