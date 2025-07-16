from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from stockops.data.controller import (
    init_controller,
    send_command,
    start_controller_in_thread,
    stop_controller_from_thread,
)
from stockops.data.historical.providers import get_historical_service
from stockops.data.streaming.providers import get_streaming_service


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Setup on startup
    stream_service = get_streaming_service("EODHD")
    hist_service = get_historical_service("EODHD")
    init_controller(stream_service, hist_service, max_streams=5)
    start_controller_in_thread()
    yield
    # Teardown on shutdown
    stop_controller_from_thread()


app = FastAPI(lifespan=lifespan)


@app.post("/send_command")
async def receive_command(request: Request):
    command = await request.json()
    await send_command(command)
    return JSONResponse(content={"status": "queued", "command": command})


@app.get("/health")
def health_check():
    return JSONResponse(content={"status": "ok"})


@app.post("/shutdown")
def shutdown_signal():
    stop_controller_from_thread()
    return JSONResponse(content={"status": "shutdown triggered"})
