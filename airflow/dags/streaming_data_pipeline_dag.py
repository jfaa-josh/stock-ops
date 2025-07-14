from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
import json

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

with DAG(
    dag_id="streaming_data_pipeline_dag",
    default_args=default_args,
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=["streaming", "historical"],
) as dag:

    # Task 1: Start streaming trades
    start_stream_trades = SimpleHttpOperator(
        task_id="start_stream_trades",
        method="POST",
        http_conn_id="controller_api",
        endpoint="send_command",
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "type": "start_stream",
            "stream_type": "trades",
            "tickers": ["SPY"],
            "duration": 10
        }),
    )

    # Task 2: Start streaming quotes
    start_stream_quotes = SimpleHttpOperator(
        task_id="start_stream_quotes",
        method="POST",
        http_conn_id="controller_api",
        endpoint="send_command",
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "type": "start_stream",
            "stream_type": "quotes",
            "tickers": ["SPY"],
            "duration": 10
        }),
    )

    # Task 3: Fetch historical intraday data
    fetch_intraday = SimpleHttpOperator(
        task_id="fetch_intraday",
        method="POST",
        http_conn_id="controller_api",
        endpoint="send_command",
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "type": "fetch_historical",
            "ticker": "SPY.US",
            "interval": "1m",
            "start": "2025-07-02 09:30",
            "end": "2025-07-02 16:00"
        }),
    )

    # Task 4: Fetch historical daily data
    fetch_daily = SimpleHttpOperator(
        task_id="fetch_daily",
        method="POST",
        http_conn_id="controller_api",
        endpoint="send_command",
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "type": "fetch_historical",
            "ticker": "SPY.US",
            "interval": "d",
            "start": "2025-07-02 09:30",
            "end": "2025-07-03 16:00"
        }),
    )

    # Task 5: Shutdown controller (optional)
    shutdown = SimpleHttpOperator(
        task_id="shutdown_controller",
        method="POST",
        http_conn_id="controller_api",
        endpoint="send_command",
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "type": "shutdown"
        }),
        trigger_rule=TriggerRule.ALL_DONE,  # Run even if previous tasks fail
    )

    # Define task dependencies
    start_stream_trades >> start_stream_quotes >> fetch_intraday >> fetch_daily >> shutdown
