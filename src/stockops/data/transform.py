import logging

from stockops.config import utils as cfg_utils  # , add additional providers here as needed
from stockops.data.ibkr.normalize import ibkr_daybar_to_isodate, to_utc_epoch, to_utc_epoch_ms
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
        elif self.provider == "IBKR":
            return {"d", "w", "m"}, {"1m", "5m", "1h"}
        raise ValueError(f"Unsupported provider: {self.provider}")

    def __call__(self, data_row: dict, interval: str = ""):
        """Note: dtypes selected for sql storage efficiency based on expected values"""
        if self.provider == "EODHD":
            return self.eodhd(data_row, interval)
        elif self.provider == "IBKR":
            return self.ibkr(data_row, interval)
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

        return transformed

    def ibkr(self, data_row: dict, interval: str) -> dict:
        """Volume/size fields are cast to int (IBKR's are float); historical bars keep
        `average`/`barCount` as IBKR-only extra columns -- matching the field-mapping decisions
        in the strategy doc's "Decisions already made" table."""
        if self.target == "to_db_writer":
            if self.data_type == "historical_interday":
                required_keys = {"date", "open", "high", "low", "close", "adjusted_close", "volume"}
                required_keys |= {"average", "barCount"}

                missing = required_keys - data_row.keys()
                if missing:
                    raise ValueError(f"Missing expected fields in historical_interday IBKR data: {missing}")

                assert interval in self.freq_interday, "Invalid interday interval for this provider."

                transformed = {
                    "date": ibkr_daybar_to_isodate(data_row["date"]),
                    **{k: data_row[k] for k in ("open", "high", "low", "close", "adjusted_close")},
                    "volume": int(data_row["volume"]),
                    "average": data_row["average"],
                    "barCount": data_row["barCount"],
                    "interval": interval,
                }

            elif self.data_type == "historical_intraday":
                required_keys = {"date", "open", "high", "low", "close", "volume", "average", "barCount"}

                missing = required_keys - data_row.keys()
                if missing:
                    raise ValueError(f"Missing expected fields in historical_intraday IBKR data: {missing}")

                assert interval in self.freq_intraday, "Invalid intraday interval for this provider."

                transformed = {
                    "timestamp_UTC_s": to_utc_epoch(data_row["date"]),
                    **{k: data_row[k] for k in ("open", "high", "low", "close")},
                    "volume": int(data_row["volume"]),
                    "average": data_row["average"],
                    "barCount": data_row["barCount"],
                    "interval": interval,
                }

            elif self.data_type == "streaming_trades":
                required_keys = {"time", "price", "size"}

                missing = required_keys - data_row.keys()
                if missing:
                    raise ValueError(f"Missing expected fields in streaming_trades IBKR data: {missing}")

                assert interval == "", "Interval invalid for spot data."

                transformed = {
                    "timestamp_UTC_ms": to_utc_epoch_ms(data_row["time"]),
                    "price": data_row["price"],
                    "volume": int(data_row["size"]),
                }

            elif self.data_type == "streaming_quotes":
                required_keys = {"time", "bidPrice", "askPrice", "bidSize", "askSize"}

                missing = required_keys - data_row.keys()
                if missing:
                    raise ValueError(f"Missing expected fields in streaming_quotes IBKR data: {missing}")

                assert interval == "", "Interval invalid for spot data."

                transformed = {
                    "timestamp_UTC_ms": to_utc_epoch_ms(data_row["time"]),
                    "ask_price": data_row["askPrice"],
                    "bid_price": data_row["bidPrice"],
                    "ask_size": int(data_row["askSize"]),
                    "bid_size": int(data_row["bidSize"]),
                }

            else:
                raise ValueError(f"Unsupported data_type for IBKR transform: {self.data_type}")

        return transformed
