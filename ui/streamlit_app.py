import streamlit as st
import logging
import sys
import time

import api_calls

# Logging setup
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
logger = logging.getLogger(__name__)


# Initialize backend only the first time this user hits the app
if "initialized" not in st.session_state:
    st.session_state.api = api_calls.APIBackend("controller_driver")
    st.session_state.initialized = True

api = st.session_state.api

# UI layout
st.title("Prefect Trigger Dashboard")

if st.button("Run Historical SPY"):
    logger.info("Button pressed: Run Historical SPY")
    try:
        deployment_name = 'some-deployment'
        schedules = None # No schedules = on‐demand runs only; add a list of DeploymentScheduleCreate
        response = api.register_deployment(deployment_name, schedules)
        logger.info(f"Button pressed, response is: {response}")
        deployment_id = response['id']

        status = "INITIAL"
        while status != "READY":
            depl_response = api.check_deployment_status(deployment_id)
            status = depl_response.get("status")
            # print(f"YO MF:  STATUS IS {status}")
            if status == "NOT_READY":
                logger.info("Deployment is in NOT_READY status.  Waiting 5 seconds, and checking again...")

            elif status is None:
                logger.exception("There is no deployment with this id!")
            time.sleep(5)

        if status == "READY":
            ticker = 'SPY.US'
            interval = '1m'
            start = '2025-07-02 09:30'
            end = '2025-07-02 16:00'
            command_type = 'fetch_historical'
            provider = 'EODHD'
            response = api.run_deployed_flow(deployment_id, ticker, interval,
                                            start, end, command_type, provider)
            flow_run_name = response['name']
    except Exception as e:
        logger.exception("Failed to trigger historical SPY flow")
        st.error("Flow trigger failed.")
        st.exception(e)

# if st.button("Run Stream SPY"):
#     logger.info("Button pressed: Run Stream SPY")
#     try:
#         result = api.trigger_flow(
#             command_type="start_stream",
#             command_payload={
#                 "stream_type": "trades",
#                 "tickers": ["SPY"],
#                 "duration": 10
#             },
#             provider="EODHD"
#         )
#         st.json(result)
#     except Exception as e:
#         logger.exception("Failed to trigger streaming SPY flow")
#         st.error("Flow trigger failed.")
#         st.exception(e)
