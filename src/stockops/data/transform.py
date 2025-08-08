import logging
from datetime import UTC, datetime, timedelta, timezone

from stockops.config import config as cfg

logger = logging.getLogger(__name__)


class TransformData:
    def __init__(self, provider: str, data_type: str):
        self.provider = provider
        self.data_type = data_type

    def __call__(self, data_row: dict):
        if self.provider == "EODHD":
            return self.eodhd(data_row)

    def eodhd(self, data_row: dict):
        def get_UTC_intraday(timestamp, gmtoffset):
            tz = timezone(timedelta(seconds=gmtoffset), name="UTC")
            dt = datetime.fromtimestamp(timestamp, tz)
            return dt.strftime(cfg.DATA_DB_DATESTR)

        def get_UTC_interday(date_str, gmtoffset):
            """
            Convert a date string 'YYYY-MM-DD' to the UTC timestamp (seconds since epoch)
            corresponding to midnight at the start of that date.  Then convert to UTC with offset.
            """
            # 1. Parse the date (naïvely, no timezone yet)
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            # 2. Tell Python it’s UTC
            dt_utc = dt.replace(tzinfo=UTC)
            # 3. Get the epoch seconds
            timestamp = int(dt_utc.timestamp())
            tz = timezone(timedelta(seconds=gmtoffset), name="UTC")  # REUSE FROM ABOVE?!?!?!?!
            dt = datetime.fromtimestamp(timestamp, tz)  # REUSE FROM ABOVE?!?!?!?!
            return dt.strftime(cfg.DATA_DB_DATESTR)  # REUSE FROM ABOVE?!?!?!?!

        def UTC_timestamp_ms(ms_timestamp: int, gmtoffset: int) -> str:
            # 1) convert ms → float seconds
            ts = ms_timestamp / 1_000.0

            # 2) build a fixed-offset tzinfo named "UTC"
            tz = timezone(timedelta(seconds=gmtoffset), name="UTC")

            # 3) convert to an aware datetime (with microsecond precision)
            dt = datetime.fromtimestamp(ts, tz)

            # 4) extract milliseconds
            ms = dt.microsecond // 1_000

            # 5) format as "YYYY-MM-DD HH:MM:SS.xxx UTC±HHMM"
            return (
                dt.strftime("%Y-%m-%d %H:%M:%S")  # up to whole seconds
                + f".{ms:03d} "  # .xxx
                + dt.strftime("%Z%z")  # UTC±offset
            )

        if self.data_type == "historical_intraday":
            # "datetime": "UTC datetime string YYYY-MM-DD HH:MM:SS for the start of the interval"
            # CONVERT TO MID POINT AND DOCUMENT

            required_keys = {"timestamp", "gmtoffset", "open", "high", "low", "close", "volume"}

            missing = required_keys - data_row.keys()
            if missing:
                logger.debug("Missing expected fields in historical_intraday EODHD data: %s", missing)
                raise

            transformed = {
                "datetime_UTC": get_UTC_intraday(data_row["timestamp"], data_row["gmtoffset"]),
                **{k: data_row[k] for k in ("open", "high", "low", "close", "volume")},
            }

        elif self.data_type == "historical_interday":
            # "date": "UTC datetime string YYYY-MM-DD for the end of the interval"
            # CONVERT TO MID POINT AND DOCUMENT

            gmtoffset = -14400  # THIS IS NOT CORRECT!!!

            required_keys = {"date", "open", "high", "low", "close", "adjusted_close", "volume"}

            missing = required_keys - data_row.keys()
            if missing:
                logger.debug("Missing expected fields in historical_interday EODHD data: %s", missing)
                raise

            transformed = {
                "datetime_UTC": get_UTC_interday(data_row["date"], gmtoffset),
                **{k: data_row[k] for k in ("open", "high", "low", "close", "adjusted_close", "volume")},
            }

        elif self.data_type == "streaming_trades":
            # "datetime": "UTC datetime string YYYY-MM-DD HH:MM:SS for the start of the interval"
            # CONVERT TO MID POINT AND DOCUMENT

            required_keys = {"t", "p", "v"}

            missing = required_keys - data_row.keys()
            if missing:
                logger.debug("Missing expected fields in streaming_trades EODHD data: %s", missing)
                raise

            transformed = {
                "datetime_UTC": UTC_timestamp_ms(data_row["t"], 0),  # Converting milliseconds to fractional seconds
                "price": data_row["p"],
                "volume": data_row["v"],
            }

        elif self.data_type == "streaming_quotes":
            # "datetime": "UTC datetime string YYYY-MM-DD HH:MM:SS for the start of the interval"
            # CONVERT TO MID POINT AND DOCUMENT

            required_keys = {"t", "ap", "bp", "as", "bs"}

            missing = required_keys - data_row.keys()
            if missing:
                logger.debug("Missing expected fields in streaming_quotes EODHD data: %s", missing)
                raise

            transformed = {
                "datetime_UTC": UTC_timestamp_ms(data_row["t"], 0),  # Converting milliseconds to fractional seconds
                "ask_price": data_row["ap"],
                "bid_price": data_row["bp"],
                "ask_size": data_row["as"],
                "bid_size": data_row["bs"],
            }

        return transformed
