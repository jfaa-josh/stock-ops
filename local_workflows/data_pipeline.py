import subprocess
import time
import requests
import sys

# Config
CONTROLLER_URL = "http://localhost:8000"
SEND_ENDPOINT = f"{CONTROLLER_URL}/send_command"
SHUTDOWN_ENDPOINT = f"{CONTROLLER_URL}/shutdown"
HEALTH_ENDPOINT = f"{CONTROLLER_URL}/health"
API_STARTUP_WAIT = 2  # seconds


def start_fastapi_subprocess():
    """Start the FastAPI controller API using uvicorn as a background subprocess."""
    print("🔧 Starting FastAPI API subprocess...")

    process = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "stockops.runtime.data_pipeline_api:app", "--host", "127.0.0.1", "--port", "8000"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,  # ensures output is string, not bytes
        bufsize=1,  # line-buffered
    )

    try:
        for _ in range(10):
            try:
                r = requests.get("http://127.0.0.1:8000/health")
                if r.status_code == 200:
                    print("✅ FastAPI service is online.")
                    return process
            except requests.ConnectionError:
                time.sleep(0.5)

        # If we reach here, FastAPI didn't start
        if process.stdout:
            print("❌ Failed to start FastAPI. Partial logs:")
            for _ in range(20):
                line = process.stdout.readline()
                if not line:
                    break
                print(line.strip())

        process.terminate()
        raise RuntimeError("❌ Failed to start FastAPI service.")

    except Exception:
        process.terminate()
        raise


def send_command_http(command: dict):
    print(f"Sending command: {command}")
    try:
        response = requests.post(SEND_ENDPOINT, json=command)
        response.raise_for_status()
        print("✅ Response:", response.json())
    except requests.RequestException as e:
        print("❌ Failed to send command:", e)


def shutdown_fastapi():
    print("Shutting down FastAPI controller...")
    try:
        response = requests.post(SHUTDOWN_ENDPOINT)
        print("Shutdown response:", response.json())
    except requests.RequestException as e:
        print("❌ Failed to shut down FastAPI:", e)


# -----------------------------------------------------------------------------
# MANUAL COMMAND BLOCKS — Run these one at a time during local dev
# -----------------------------------------------------------------------------
if main := __name__ == "__main__":
    # --- Start FastAPI controller subprocess
    api_proc = start_fastapi_subprocess()

    # --- Example 1: Start streaming trades
    send_command_http({"type": "start_stream", "stream_type": "trades", "tickers": ["SPY"], "duration": 10})

    # --- Example 2: Start streaming quotes
    send_command_http({"type": "start_stream", "stream_type": "quotes", "tickers": ["SPY"], "duration": 10})

    # --- Example 3: Fetch historical intraday data
    send_command_http(
        {
            "type": "fetch_historical",
            "ticker": "SPY.US",
            "interval": "1m",
            "start": "2025-07-02 09:30",
            "end": "2025-07-02 16:00",
        }
    )

    # --- Example 4: Fetch historical daily data
    send_command_http(
        {
            "type": "fetch_historical",
            "ticker": "SPY.US",
            "interval": "d",
            "start": "2025-07-02 09:30",
            "end": "2025-07-03 16:00",
        }
    )

    # --- Shutdown FastAPI controller subprocess (also shutsdown any running controller threads)
    shutdown_fastapi()
    print("Terminating FastAPI subprocess...")
    api_proc.terminate()  # <- Cross-platform safe
    api_proc.wait()
    print("✅ API subprocess terminated cleanly.")


### THIS NEEDS TO CHANGE USING METADATA SO I CAN RUN THIS THROUGH CONTROLLER ###
# from stockops.data.sql_db import SQLiteReader
# from stockops.config.config import RAW_STREAMING_DIR, RAW_HISTORICAL_DIR

# reader = SQLiteReader(RAW_HISTORICAL_DIR/'intraday_2025-07_EODHD.db')
# print(reader.list_tables())  # ['trades', 'quotes', 'stream_metadata']
# spy_intradata = reader.fetch_all("SPY.US")

# reader = SQLiteReader(RAW_HISTORICAL_DIR/'interday_EODHD.db')
# print(reader.list_tables())  # ['trades', 'quotes', 'stream_metadata']
# spy_interdata = reader.fetch_all("SPY.US")

# reader = SQLiteReader(RAW_STREAMING_DIR/'interday_EODHD.db')
# trades = reader.fetch_all("trades")
# quotes = reader.fetch_where("quotes", "s = ?", ["SPY"])
# metadata = reader.fetch_metadata()
# # Raw SQL
# tickers = reader.execute_raw_query("SELECT DISTINCT tickers FROM stream_metadata")
