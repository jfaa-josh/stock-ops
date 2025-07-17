import subprocess
import sys
import os

def run_single_test_file(filename: str):
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    test_file = os.path.join(project_root, "tests", filename)

    # Full CLI-style subprocess run
    cmd = [
        sys.executable,
        "-m", "pytest",
        "-s",
        "--log-cli-level=INFO",
        "--cov=src",
        "--cov=local_workflows",
        "--cov-report=term-missing",
        "--cov-branch",
        "--cov-config=pyproject.toml",
        test_file
    ]
    print(f"🔧 Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=project_root)
    print(f"✅ Done: {filename} (exit={result.returncode})")
    return result.returncode

# Example usage
run_single_test_file("test_data_pipeline_api_controller.py")
run_single_test_file("test_datapipe.py")
run_single_test_file("test_controller_isolated.py")
