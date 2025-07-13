from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import asyncio
import logging

from stockops.data.controller import (
    init_controller,
    run_controller_once,
)
from stockops.data.providers import get_streaming_service

# -----------------------------------------------------------------------------
# DAG CONFIGURATION
# -----------------------------------------------------------------------------
MAX_STREAMS = 3

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="streaming_data_pipeline",
    default_args=default_args,
    schedule_interval=None,  # Manual trigger or external scheduler
    catchup=False,
    description="Run streaming data API calls through controller",
    params={  # Default command list (can be overridden in web UI)
        "commands": [
            {
                "type": "start_stream",
                "stream_type": "trades",
                "tickers": ["SPY"],
                "duration": 10
            },
            {
                "type": "start_stream",
                "stream_type": "trades",
                "tickers": ["QQQ"],
                "duration": 12
            }
        ]
    }
)

# -----------------------------------------------------------------------------
# TASK FUNCTION
# -----------------------------------------------------------------------------
def run_streaming_pipeline(**context):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    service = get_streaming_service("EODHD")
    init_controller(service, historical_service=None, max_streams=MAX_STREAMS)

    commands = context["params"]["commands"]
    asyncio.run(run_controller_once(commands))

# -----------------------------------------------------------------------------
# PYTHON TASK
# -----------------------------------------------------------------------------
run_streams_task = PythonOperator(
    task_id="run_streaming_controller",
    python_callable=run_streaming_pipeline,
    provide_context=True,
    dag=dag,
)
