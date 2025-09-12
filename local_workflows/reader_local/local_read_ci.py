import pandas as pd
from pathlib import WindowsPath, Path
import logging

from stockops.data.database.reader import ReadProcess
from stockops.config import config


logger = logging.getLogger(__name__)

def main():
    # Logging setup
    logging.basicConfig(
        level=logging.INFO,  # or DEBUG
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    # Static
    provider = "EODHD"
    exchange = "US"

    def run(provider, exchange, *, data_type, ticker, interval, start_date, end_date):
        reader = ReadProcess(provider, data_type, exchange)
        data = reader.read_sql(ticker, interval, start_date, end_date)
        return reader.get_df(data)

    # Daily historical VOO
    daily_test = {'data_type': "historical_interday", 'start_date': '2024-01-01',
                'end_date': '2025-01-01', 'ticker': 'VOO', 'interval': 'd'}

    df_day_voo = run(provider, exchange, **daily_test)

    # Daily historical SPY
    daily_test = {'data_type': "historical_interday", 'start_date': '2024-01-01',
                'end_date': '2025-01-01', 'ticker': 'SPY', 'interval': 'd'}

    df_day_spy = run(provider, exchange, **daily_test)

    # Hourly historical:
    hourly_test = {'data_type': "historical_intraday", 'start_date': '2025-07-01 09:30',
                'end_date': '2025-08-01 16:00', 'ticker': 'SPY', 'interval': '1h'}

    df_hr = run(provider, exchange, **hourly_test)

    # Streaming:
    stream_test = {'data_type': "streaming", 'start_date': '2025-08-17 00:00',
                'end_date': '2025-08-19 23:59', 'ticker': 'SPY', 'interval': None}

    df_stream = run(provider, exchange, **stream_test)

    # Parse test_data.txt to pd.dfs so they can be compared to reader output dfs
    test_data_path = config.DATA_RAW_DIR/'inputs'/'test_data.txt'

    safe_globals = {"WindowsPath": WindowsPath}   # expose only what’s needed
    records = []
    with open(test_data_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                d = eval(line.strip(), safe_globals, {})
                if isinstance(d.get("db_path"), Path):
                    d["db_path"] = str(d["db_path"])
                records.append(d)

    df_raw = pd.DataFrame([r["row"] | {"db_path": r["db_path"], "table": r["table"]}
                        for r in records])

    mask_day  = df_raw["db_path"].str.contains("historical_interday", regex=False)
    mask_hour = df_raw["db_path"].str.contains("historical_intraday", regex=False)
    mask_stream = df_raw["db_path"].str.contains("streaming", regex=False)

    expected_day = df_raw[mask_day].dropna(axis="columns", how="all").drop_duplicates()
    expected_hour = df_raw[mask_hour].dropna(axis="columns", how="all").drop_duplicates()
    expected_hour = expected_hour.dropna(subset=expected_hour.columns.difference(['timestamp_UTC_s',
                                                                                'interval', "db_path", "table"]),
                                                                                how='all')
    expected_stream = df_raw[mask_stream].dropna(axis="columns", how="all").drop_duplicates()

    def check_shape(name, a, b, drop_a=['version'], drop_b=[]):
        df_a = a.drop(columns=drop_a, errors='ignore')
        df_b = b.drop(columns=drop_b, errors='ignore')

        logger.info('Checking reader output against ground truth via direct parsing of test_data.txt...')
        if df_a.shape == df_b.shape:
            logger.info("%s reader shape matches ground truth (%s)", name, df_a.shape)
        else:
            logger.error("%s reader shape mismatches ground truth: got %s, expected %s", name,
                         df_a.shape, df_b.shape)
            raise AssertionError(f"{name}: shape {df_a.shape} != {df_b.shape}")

    check_shape('VOO daily', df_day_voo, expected_day.query("table=='VOO'"),
                    drop_b=['db_path','table','interval'])

    check_shape('SPY daily', df_day_spy, expected_day.query("table=='SPY'"),
                    drop_b=['db_path','table','interval'])

    check_shape('SPY hourly', df_hr, expected_hour.query("table=='SPY'"),
                    drop_b=['db_path','table'])

    check_shape('SPY streaming', df_stream, expected_stream.query("table=='SPY'"),
                    drop_b=['db_path','table'])


if __name__ == "__main__":
    main()
