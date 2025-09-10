import pytest


@pytest.mark.integration
def test_smoke():
    assert True


# if __name__ == "__main__":
#     logging.basicConfig(
#         level=logging.DEBUG,
#         format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
#         handlers=[logging.StreamHandler(sys.stdout)],
#         force=True,
#     )

#     run_deployment(
#         name="controller-driver-flow/local-controller",
#         parameters={
#             "commands": [
#                 {"stream_type": "trades", "tickers": ["SPY"], "duration": 10},
#                 {"stream_type": "quotes", "tickers": ["SPY"], "duration": 10}
#             ],
#             "command_type": "start_stream",
#             "provider": "EODHD"
#         },
#     )

#     run_deployment(
#         name="controller-driver-flow/local-controller",
#         parameters={
#             "commands": [
#                 {
#                     "ticker": "SPY.US",
#                     "interval": "1m",
#                     "start": "2025-07-02 09:30",
#                     "end": "2025-07-02 16:00"
#                 },
#                 {
#                     "ticker": "SPY.US",
#                     "interval": "d",
#                     "start": "2025-07-02 09:30",
#                     "end": "2025-07-03 16:00"
#                 }
#             ],
#             "command_type": "fetch_historical",
#             "provider": "EODHD"
#         },
#     )
