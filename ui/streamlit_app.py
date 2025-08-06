import streamlit as st
import logging
import sys

import api_calls as api

# Logging setup
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
logger = logging.getLogger(__name__)

# Initialize prefect API
resp = api.register_controller_flow('controller_driver_flow')
flow_id = resp['id']

# UI layout
st.title("Prefect Trigger Dashboard")

if st.button("Run Historical SPY"):
    logger.info("Button pressed: Run Historical SPY")
    try:
        deployment_name = 'some-deployment'
        response = api.register_deployment(flow_id, deployment_name)
        logger.info(f"Button pressed, response is: {response}")


        # result = api.trigger_flow(
        #     command_type="fetch_historical",
        #     command_payload={
        #         "ticker": "SPY.US",
        #         "interval": "1m",
        #         "start": "2025-07-02 09:30",
        #         "end": "2025-07-02 16:00"
        #     },
        #     provider="EODHD"
        # )
        # st.json(result)
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
