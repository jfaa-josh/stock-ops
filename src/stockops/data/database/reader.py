import logging
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from stockops.config import config
from stockops.config import utils as cfg_utils
from stockops.data.database.sql_db import SQLiteReader
from stockops.data.database.utils import set_ts_col
from stockops.data.utils import get_filenames_for_dates, tzstr_to_utcts, validate_isodatestr, validate_utc_ts

logger = logging.getLogger(__name__)


class ReadProcess:
    def __init__(self, provider: str, data_type: str, exchange: str = "US"):
        self.provider = provider
        self.data_type = data_type
        self.exchange = exchange
        self.cfg_utils = cfg_utils.ProviderConfig(provider, exchange)
        self.tz = ZoneInfo(self.cfg_utils.tz_str)

    def read_sql(self, ticker: str, interval: str, start_date: str, end_date: str) -> list[dict]:
        """query sql database by date range, interval, and ticker; return raw data as list[dict]"""
        if self.provider == "EODHD":
            return self.read_eodhd(ticker, interval, start_date, end_date)
        raise ValueError(f"Unsupported provider: {self.provider}")

    def read_eodhd(self, ticker: str, interval: str, start_date: str, end_date: str) -> list[dict]:
        def convert_to_table_ts(datestr: str, precision: str = "s") -> int:
            if self.data_type == "historical_intraday":
                ts = tzstr_to_utcts(datestr, "%Y-%m-%d %H:%M", self.tz)
            elif self.data_type == "streaming":
                ts_s = tzstr_to_utcts(datestr, "%Y-%m-%d %H:%M", self.tz)
                ts = ts_s * 1000
            return validate_utc_ts(ts, precision)

        start: str | int
        end: str | int
        root = config.RAW_HISTORICAL_DIR
        if self.data_type == "historical_interday":
            start = validate_isodatestr(start_date)
            end = validate_isodatestr(end_date)
        elif self.data_type == "historical_intraday":
            start = convert_to_table_ts(start_date)
            end = convert_to_table_ts(end_date)
        elif self.data_type == "streaming":
            root = config.RAW_STREAMING_DIR
            start = convert_to_table_ts(start_date, precision="ms")
            end = convert_to_table_ts(end_date, precision="ms")

        filenames = get_filenames_for_dates(self.data_type, self.tz, self.provider, self.exchange, (start, end))
        db_files = [Path(root) / str(file) for file in filenames]

        # FIX THIS ONCE I FIX TABLE NAMING SO THAT IN ALL CASES TABLE IS JUST TICKER NOT TICKER.EXCHANGE!!!!#########
        # THIS IS DONE CAN DELETE ONCE I HAVE NEW .DB
        table = ticker
        if self.data_type != "streaming":
            table = f"{ticker}.{self.exchange}"
        #############################################################################################################

        self.ts_col = set_ts_col(self.provider, self.data_type)
        sql_reader = SQLiteReader(self.ts_col)

        rows: list[dict] = sql_reader.read_dt_range(db_files, table, interval, start, end)

        return rows

    def get_df(self, data: list[dict]) -> pd.DataFrame:
        def set_index(df: pd.DataFrame) -> pd.DataFrame:
            if self.provider == "EODHD":
                if self.data_type == "historical_interday":
                    df["date"] = pd.to_datetime(df[self.ts_col], format="%Y-%m-%d").dt.tz_localize(self.tz)
                elif self.data_type == "historical_intraday":
                    df["date"] = pd.to_datetime(df[self.ts_col], unit="s", utc=True).dt.tz_convert(self.tz)
                elif self.data_type == "streaming":
                    df["date"] = pd.to_datetime(df[self.ts_col], unit="ms", utc=True).dt.tz_convert(self.tz)

            df = df.set_index("date").sort_index()
            return df

        df = pd.DataFrame(data)

        if not hasattr(self, "ts_col"):
            set_ts_col(self.provider, self.data_type)
        df = set_index(df)

        ### DELETE THESE AFTER I FIX SQL_DB BUGS: ######################################################
        has_interval = "interval" in df.columns
        cols_to_consider = [c for c in df.columns if c not in ("msg_id", "interval")]

        primary_sort = cols_to_consider.copy()
        if has_interval:
            primary_sort.append("interval")
        if "msg_id" in df.columns:
            primary_sort.append("msg_id")

        df_dedup = df.sort_values(primary_sort, kind="mergesort")
        df_dedup = df_dedup.drop_duplicates(subset=cols_to_consider, keep="first").reset_index(drop=True)
        if "msg_id" in df_dedup.columns:
            major_minor = df_dedup["msg_id"].astype(str).str.split("-", n=1, expand=True)
            df_dedup["_msg_major"] = pd.to_numeric(major_minor[0], errors="coerce").fillna(-1)
            df_dedup["_msg_minor"] = pd.to_numeric(major_minor[1], errors="coerce").fillna(-1)
        else:
            df_dedup["_msg_major"] = -1
            df_dedup["_msg_minor"] = -1

        sort_keys = [self.ts_col] + (["interval"] if has_interval else []) + ["_msg_major", "_msg_minor"]
        df_dedup = df_dedup.sort_values(sort_keys, kind="mergesort")

        gb_keys = [self.ts_col] + (["interval"] if has_interval else [])
        df_dedup["version"] = (df_dedup.groupby(gb_keys).cumcount() + 1).astype("int64")

        df_dedup = df_dedup.sort_index(kind="mergesort")
        df_dedup = df_dedup.drop(columns=["_msg_major", "_msg_minor"])

        df = df_dedup.copy()
        print(df.head())
        ###################################################################################################

        return df
