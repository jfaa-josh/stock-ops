from fastapi import FastAPI, Request

from stockops.data.controller import (
    init_controller,
    send_command,
    start_controller_in_thread,
)
from stockops.data.historical.providers import get_historical_service
from stockops.data.streaming.providers import get_streaming_service

app = FastAPI()


@app.on_event("startup")
def setup_controller():
    stream_service = get_streaming_service("EODHD")
    hist_service = get_historical_service("EODHD")
    init_controller(stream_service, hist_service, max_streams=5)
    start_controller_in_thread()


@app.post("/send_command")
async def receive_command(request: Request):
    command = await request.json()
    await send_command(command)
    return {"status": "queued", "command": command}
