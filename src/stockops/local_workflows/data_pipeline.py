import requests

CONTROLLER_URL = "http://localhost:8000/send_command"  # Change if running in Docker


def send_command_http(command: dict):
    print(f"Sending command: {command}")
    try:
        response = requests.post(CONTROLLER_URL, json=command)
        response.raise_for_status()
        print("✅ Response:", response.json())
    except requests.RequestException as e:
        print("❌ Failed to send command:", e)


# -----------------------------------------------------------------------------
# MANUAL COMMAND BLOCKS — Run these one at a time during local dev
# -----------------------------------------------------------------------------

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

# --- Example 5: Shutdown (optional, clean shutdown signal)
send_command_http({"type": "shutdown"})


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
