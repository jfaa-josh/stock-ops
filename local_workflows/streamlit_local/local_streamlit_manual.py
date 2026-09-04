import os, sys, subprocess

os.environ["TEST_MODE"] = "1"
os.environ["EODHD_UI_AVAILABLE"] = "true"
os.environ["IBKR_UI_AVAILABLE"] = "true"
os.environ["IBKR_TRADING_MODE"] = "paper"
sys.exit(
    subprocess.call([sys.executable, "-m", "streamlit", "run", "datapipe_ui/frontend.py", "--server.port", "8501"])
)
