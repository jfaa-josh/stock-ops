import streamlit as st
import requests

API_URL = "http://prefect-server:4201/api"

def trigger_flow(command_type, command_payload, provider="EODHD"):
    response = requests.post(
        f"{API_URL}/deployments/create_flow_run",
        json={
            "name": "manual-trigger",
            "parameters": {
                "command_type": command_type,
                "commands": [command_payload],
                "provider": provider
            },
            "deployment_id": "controller-driver-flow/local-controller"
        }
    )
    return response.json()


st.title("Prefect Trigger Dashboard")

if st.button("Run Historical SPY"):
    result = trigger_flow(
        command_type="fetch_historical",
        command_payload={
            "ticker": "SPY.US",
            "interval": "1m",
            "start": "2025-07-02 09:30",
            "end": "2025-07-02 16:00"
        },
        provider="EODHD"  # Optional override
    )
    st.json(result)

if st.button("Run Stream SPY"):
    result = trigger_flow(
        command_type="start_stream",
        command_payload={
            "stream_type": "trades",
            "tickers": ["SPY"],
            "duration": 10
        },
        provider="EODHD"  # Optional override
    )
    st.json(result)
