import logging

# from zoneinfo import ZoneInfo
from stockops.config import utils as cfg_utils  # , add additional providers here as needed
from stockops.data.utils import validate_isodatestr, validate_utc_ts

logger = logging.getLogger(__name__)


class TransformData:
    def __init__(self, provider: str, data_type: str, target: str, exchange: str = "US"):
        self.provider = provider
        self.data_type = data_type
        self.target = target
        self.cfg_utils = cfg_utils.ProviderConfig(provider, exchange)
        self.tz_str = self.cfg_utils.tz_str
        self.freq_interday, self.freq_intraday = self.set_freqs()

    def set_freqs(self):
        if self.provider == "EODHD":
            return {"d", "w", "m"}, {"1m", "5m", "1h"}
        raise ValueError(f"Unsupported provider: {self.provider}")

    def __call__(self, data_row: dict, interval: str = ""):
        if self.provider == "EODHD":
            return self.eodhd(data_row, interval)
        raise ValueError(f"Unsupported provider: {self.provider}")

    def eodhd(self, data_row: dict, interval: str) -> dict:
        if self.target == "to_db_writer":
            if self.data_type == "historical_interday":
                required_keys = {"date", "open", "high", "low", "close", "adjusted_close", "volume"}

                missing = required_keys - data_row.keys()
                if missing:
                    logger.debug("Missing expected fields in historical_interday EODHD data: %s", missing)
                    raise

                assert interval in self.freq_interday, "Invalid interday interval for this provider."

                transformed = {
                    "date": validate_isodatestr(data_row["date"]),
                    **{k: data_row[k] for k in ("open", "high", "low", "close", "adjusted_close", "volume")},
                    "interval": interval,
                }

            elif self.data_type == "historical_intraday":
                required_keys = {"timestamp", "open", "high", "low", "close", "volume"}

                missing = required_keys - data_row.keys()
                if missing:
                    logger.debug("Missing expected fields in historical_intraday EODHD data: %s", missing)
                    raise

                assert interval in self.freq_intraday, "Invalid intraday interval for this provider."

                transformed = {
                    "timestamp_UTC_s": validate_utc_ts(data_row["timestamp"], precision="s"),
                    **{k: data_row[k] for k in ("open", "high", "low", "close", "volume")},
                    "interval": interval,
                }

            elif self.data_type == "streaming_trades":
                required_keys = {"t", "p", "v"}

                missing = required_keys - data_row.keys()
                if missing:
                    logger.debug("Missing expected fields in streaming_trades EODHD data: %s", missing)
                    raise

                assert interval == "", "Interval invalid for spot data."

                transformed = {
                    "timestamp_UTC_ms": validate_utc_ts(data_row["t"], precision="ms"),
                    "price": data_row["p"],
                    "volume": data_row["v"],
                }

            elif self.data_type == "streaming_quotes":
                required_keys = {"t", "ap", "bp", "as", "bs"}

                missing = required_keys - data_row.keys()
                if missing:
                    logger.debug("Missing expected fields in streaming_quotes EODHD data: %s", missing)
                    raise

                assert interval == "", "Interval invalid for spot data."

                transformed = {
                    "timestamp_UTC_ms": validate_utc_ts(data_row["t"], precision="ms"),
                    "ask_price": data_row["ap"],
                    "bid_price": data_row["bp"],
                    "ask_size": data_row["as"],
                    "bid_size": data_row["bs"],
                }

        elif self.target == "from_db_reader":
            if self.data_type == "historical_interday":
                # HERE NO DATE CONVERSION IS NEEDED
                transformed = dict(data_row)  # THIS IS A DUMMY TO PASS TYPECHECKING!!!
            elif self.data_type == "historical_intraday":
                # HERE I NEED TO CONVERT TS TO HUMAN READABLE DATE OF CORRECT ATOMIC UNIT IN LOCAL EXCHANGE TZ:
                # - '1m': "%Y-%m-%d %H:%M"
                # - '5m': "%Y-%m-%d %H:%M"
                # - '1h': "%Y-%m-%d %H"
                # self.tz_str
                transformed = dict(data_row)  # THIS IS A DUMMY TO PASS TYPECHECKING!!!
            elif self.data_type == "streaming":
                # HERE I NEED TO CONVERT TS TO FRACTION SECONDS (MS TO S) THEN TO HUMAN READABLE DATE:
                # "%Y-%m-%d %H:%M:%S.%f"
                # self.tz_str
                transformed = dict(data_row)  # THIS IS A DUMMY TO PASS TYPECHECKING!!!
        else:
            raise ValueError(f"Unsupported target: {self.target}")

        return transformed
