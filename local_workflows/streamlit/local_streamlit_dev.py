import os, sys, subprocess

os.environ["TEST_MODE"] = "1"
sys.exit(
    subprocess.call([sys.executable, "-m", "streamlit", "run", "datapipe_ui/frontend.py", "--server.port", "8501"])
)
