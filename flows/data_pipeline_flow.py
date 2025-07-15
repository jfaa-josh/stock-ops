from prefect import flow, task
import httpx

CONTROLLER_URL = "http://localhost:8000/send_command"  # Change for Docker networking


@task
def send_command(command: dict):
    print(f"Sending command: {command}")
    try:
        response = httpx.post(CONTROLLER_URL, json=command)
        response.raise_for_status()
        print("✅ Response:", response.json())
    except httpx.RequestError as e:
        print("❌ Failed to send command:", e)


@flow
def controller_command_flow():
    # --- Example 1: Start streaming trades
    send_command.submit({"type": "start_stream", "stream_type": "trades", "tickers": ["SPY"], "duration": 10})

    # --- Example 2: Start streaming quotes
    send_command.submit({"type": "start_stream", "stream_type": "quotes", "tickers": ["SPY"], "duration": 10})

    # --- Example 3: Fetch historical intraday data
    send_command.submit(
        {
            "type": "fetch_historical",
            "ticker": "SPY.US",
            "interval": "1m",
            "start": "2025-07-02 09:30",
            "end": "2025-07-02 16:00",
        }
    )

    # --- Example 4: Fetch historical daily data
    send_command.submit(
        {
            "type": "fetch_historical",
            "ticker": "SPY.US",
            "interval": "d",
            "start": "2025-07-02 09:30",
            "end": "2025-07-03 16:00",
        }
    )

    # --- Example 5: Shutdown
    send_command.submit({"type": "shutdown"})


if __name__ == "__main__":
    controller_command_flow()
