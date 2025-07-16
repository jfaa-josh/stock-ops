import pytest
import sys
import os

def run_single_test_file(filename: str):
    # Add project root to sys.path
    current_file = os.path.abspath(__file__)  # /local_workflows/pytests.py
    project_root = os.path.dirname(os.path.dirname(current_file))  # one level up

    sys.path.insert(0, project_root)
    args = [
        "-s", # Disable output capturing
        "--log-cli-level=INFO",  # Set log level for CLI output
        "--cov=src",                          # Measure coverage on src/
        "--cov=local_workflows",         #
        "--cov-report=term-missing",         # Show lines not covered
        "--cov-branch",                     # Branch coverage
        "--cov-config=pyproject.toml",      # Pick up config
        f"tests/{filename}",             # Replace with your file path
    ]

    sys.exit(pytest.main(args))


run_single_test_file('test_data_pipeline_api_controller.py')

run_single_test_file('test_datapipe.py')
