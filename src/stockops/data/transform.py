import logging
from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

from stockops.config import eodhd_config  # , add additional providers here as needed

logger = logging.getLogger(__name__)

_CONFIG_MAP = {
    "EODHD": eodhd_config,
}


class TransformData:
    def __init__(self, provider: str, data_type: str, target: str, exchange: str = "US"):
        self.provider = provider
        self.data_type = data_type
        self.target = target
        self.exchange = exchange
        self.cfg = self.set_cfg()
        self.tz = self.get_tz()

    def set_cfg(self):
        try:
            return _CONFIG_MAP[self.provider]
        except KeyError as err:
            raise ValueError(f"Unsupported provider: {self.provider!r}. Supported: {list(_CONFIG_MAP)}") from err

    def get_tz(self):
        tz_str = self.cfg.EXCHANGE_METADATA[self.exchange]["Timezone"]
        return ZoneInfo(tz_str)

    def __call__(self, data_row: dict):
        if self.provider == "EODHD":
            return self.eodhd(data_row)

    def eodhd(self, data_row: dict):
        def validate_iso_date(s: str) -> str:
            """
            Parse a date in “YYYY-MM-DD” form and return a date object.
            Raises ValueError if the format is wrong or the date is invalid.
            """
            date.fromisoformat(s)
            return s

        def validate_utc_ts(ts: int, precision: str) -> int:
            """
            Ensure ts is an integer Unix timestamp in UTC.
            If precision is 's', ts is in seconds; if 'ms', ts is in milliseconds.
            """
            if not isinstance(ts, int):
                raise TypeError(f"Timestamp must be int, got {type(ts).__name__}")

            if precision == "s":
                datetime.fromtimestamp(ts, tz=UTC)
            elif precision == "ms":
                datetime.fromtimestamp(ts / 1000.0, tz=UTC)
            else:
                raise ValueError(f"Unsupported precision {precision!r}, expected 's' or 'ms'")
            return ts

        if self.target == "to_db_writer":
            if self.data_type == "historical_interday":
                required_keys = {"date", "open", "high", "low", "close", "adjusted_close", "volume"}

                missing = required_keys - data_row.keys()
                if missing:
                    logger.debug("Missing expected fields in historical_interday EODHD data: %s", missing)
                    raise

                transformed = {
                    "date_exchangetz": validate_iso_date(data_row["date"]),
                    **{k: data_row[k] for k in ("open", "high", "low", "close", "adjusted_close", "volume")},
                }

            elif self.data_type == "historical_intraday":
                required_keys = {"timestamp", "open", "high", "low", "close", "volume"}

                missing = required_keys - data_row.keys()
                if missing:
                    logger.debug("Missing expected fields in historical_intraday EODHD data: %s", missing)
                    raise

                transformed = {
                    "timestamp_UTC_s": validate_utc_ts(data_row["timestamp"], precision="s"),
                    **{k: data_row[k] for k in ("open", "high", "low", "close", "volume")},
                }

            elif self.data_type == "streaming_trades":
                required_keys = {"t", "p", "v"}

                missing = required_keys - data_row.keys()
                if missing:
                    logger.debug("Missing expected fields in streaming_trades EODHD data: %s", missing)
                    raise

                transformed = {
                    "timestamp_UTC_ms": validate_utc_ts(data_row["timestamp"], precision="ms"),
                    "price": data_row["p"],
                    "volume": data_row["v"],
                }

            elif self.data_type == "streaming_quotes":
                required_keys = {"t", "ap", "bp", "as", "bs"}

                missing = required_keys - data_row.keys()
                if missing:
                    logger.debug("Missing expected fields in streaming_quotes EODHD data: %s", missing)
                    raise

                transformed = {
                    "timestamp_UTC_ms": validate_utc_ts(data_row["timestamp"], precision="ms"),
                    "ask_price": data_row["ap"],
                    "bid_price": data_row["bp"],
                    "ask_size": data_row["as"],
                    "bid_size": data_row["bs"],
                }

        elif self.target == "from_db_reader":
            if self.data_type == "historical_interday":
                # HERE NO DATE CONVERSION IS NEEDED
                pass
            elif self.data_type == "historical_intraday":
                # HERE I NEED TO CONVERT TS TO HUMAN READABLE DATE OF CORRECT ATOMIC UNIT IN LOCAL EXCHANGE TZ:
                # - '1m': "%Y-%m-%d %H:%M"
                # - '5m': "%Y-%m-%d %H:%M"
                # - '1h': "%Y-%m-%d %H"
                # self.tz
                pass
            elif self.data_type == "streaming":
                # HERE I NEED TO CONVERT TS TO FRACTION SECONDS (MS TO S) THEN TO HUMAN READABLE DATE:
                # "%Y-%m-%d %H:%M:%S.%f"
                # self.tz
                pass

        return transformed
