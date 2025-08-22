from zoneinfo import ZoneInfo
from typing import List, Tuple, Sequence
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt

from stockops.data.database.sql_db import SQLiteReader
# from stockops.data.transform import TransformData
from stockops.data.utils import tzstr_to_utcts, validate_isodatestr, validate_utc_ts, period_from_unix, normalize_ts_to_seconds, utcts_to_tzstr
from stockops.config import config, utils as cfg_utils


def get_df(provider, data_type, exchange, ticker, interval, start_in, end_in):
    def convert_to_table_ts(datestr: str, tz: ZoneInfo, precision: str = 's') -> int:
        if data_type == "historical_intraday":
            ts = tzstr_to_utcts(datestr, "%Y-%m-%d %H:%M", tz)
        elif data_type == "streaming":
            ts_s = tzstr_to_utcts(datestr, "%Y-%m-%d %H:%M", tz)
            ts = ts_s * 1000
        return validate_utc_ts(ts, precision)

    def get_filenames(filename_dates: Sequence[Tuple[str, ...]] | None, data_type: str,
                    provider: str, exchange: str) -> List[str]:
        if data_type == "historical_interday":
            return [f"{data_type}_{provider}_{exchange}.db"]

        elif data_type == "historical_intraday" and filename_dates is not None:
            return [f"{data_type}_{provider}_{exchange}_{y}_{m}.db" for (y, m) in filename_dates]

        elif data_type == "streaming" and filename_dates is not None:
            return [f"{data_type}_{provider}_{exchange}_{y}_{m}_{d}.db" for (y, m, d) in filename_dates]

        raise ValueError(f"Unsupported data_type: {data_type!r}")

    if provider == "EODHD":
        cfg = cfg_utils.ProviderConfig(provider, exchange)
        tz = ZoneInfo(cfg.tz_str)

        root = config.RAW_HISTORICAL_DIR
        if data_type == "historical_interday":
            ts_col = "date"
            start = validate_isodatestr(start_in)
            end = validate_isodatestr(end_in)
            filename_dates = None
        elif data_type == "historical_intraday":
            ts_col = "timestamp_UTC_s"
            start = convert_to_table_ts(start_in, tz)
            end = convert_to_table_ts(end_in, tz)
            filename_dates = period_from_unix(start, end, tz, precision = 'mo')
        elif data_type == "streaming":
            root = config.RAW_STREAMING_DIR
            ts_col = "timestamp_UTC_ms"
            start = convert_to_table_ts(start_in, tz, precision = 'ms')
            end = convert_to_table_ts(end_in, tz, precision = 'ms')
            filename_dates = period_from_unix(start, end, tz, precision = 'day')

    filenames = get_filenames(filename_dates, data_type, provider, exchange)
    db_files = [Path(root) / str(file) for file in filenames]

    reader = SQLiteReader(ts_col)

    # FIX THIS ONCE I FIX TABLE NAMING SO THAT IN ALL CASES TABLE IS JUST TICKER NOT TICKER.EXCHANGE!!!!#########
    table = ticker
    if data_type != "streaming": table = f'{ticker}.{exchange}'
    #############################################################################################################

    df = reader.read_dt_range(db_files, table, interval, start, end)

    ### DELETE THESE AFTER I FIX SQL_DB BUGS: ######################################################
    has_interval = 'interval' in df.columns
    cols_to_consider = [c for c in df.columns if c not in ('msg_id', 'interval')]

    primary_sort = cols_to_consider.copy()
    if has_interval:
        primary_sort.append('interval')
    if 'msg_id' in df.columns:
        primary_sort.append('msg_id')

    df_dedup = df.sort_values(primary_sort, kind='mergesort')
    df_dedup = df_dedup.drop_duplicates(subset=cols_to_consider, keep='first').reset_index(drop=True)
    if 'msg_id' in df_dedup.columns:
        major_minor = df_dedup['msg_id'].astype(str).str.split('-', n=1, expand=True)
        df_dedup['_msg_major'] = pd.to_numeric(major_minor[0], errors='coerce').fillna(-1)
        df_dedup['_msg_minor'] = pd.to_numeric(major_minor[1], errors='coerce').fillna(-1)
    else:
        df_dedup['_msg_major'] = -1
        df_dedup['_msg_minor'] = -1

    sort_keys = [ts_col] + (['interval'] if has_interval else []) + ['_msg_major', '_msg_minor']
    df_dedup = df_dedup.sort_values(sort_keys, kind='mergesort')

    gb_keys = [ts_col] + (['interval'] if has_interval else [])
    df_dedup['version'] = (df_dedup.groupby(gb_keys).cumcount() + 1).astype('int64')

    df_dedup = df_dedup.sort_index(kind='mergesort')
    df_dedup = df_dedup.drop(columns=['_msg_major', '_msg_minor'])

    df = df_dedup.copy()
    print(df.head())
    ###################################################################################################

    ### DELETE THESE AFTER I FINISH XFORMER: ####
    # transformer = TransformData(provider, data_type, "from_db_reader", exchange)
    # if df[''].apply()
    def conf_date(ts): ##!!!!!ADD THIS TO XFOMER!
        n_ts = normalize_ts_to_seconds(int(ts))
        if data_type == "historical_intraday":
            return utcts_to_tzstr(n_ts, "%Y-%m-%d %H:%M", tz)
        elif data_type == "streaming":
            return utcts_to_tzstr(n_ts, "%Y-%m-%d %H:%M:%S.%f", tz)

    if data_type != "historical_interday":
        df[ts_col] = df[ts_col].apply(conf_date)
        df = df.rename(columns={ts_col: "date"})
    #############################################

    return df

# Static
provider = "EODHD"
exchange = "US"



# DAILY:
data_type = "historical_interday"

start_in = '2025-07-25'
end_in = '2025-08-21'

ticker = 'SPY'
interval = 'd'

#### I NEED TO GET TS_COL OUT OF THE RETURN FUNCTION HERE
df_day = get_df(provider, data_type, exchange, ticker, interval, start_in, end_in)


### DELETE THESE AFTER I FIX SQL_DB BUGS: ###

df_day = df_day[df_day['version'].isin([1, 2])]
df_day = df_day.reset_index(drop=True)
print(df_day.head())
#############################################

# HOURLY:
data_type = "historical_intraday"

start_in = '2025-08-02 09:30'
end_in = '2025-09-21 16:00'

ticker = 'SPY'
interval = '1h'

#### I NEED TO GET TS_COL OUT OF THE RETURN FUNCTION HERE
df_hr = get_df(provider, data_type, exchange, ticker, interval, start_in, end_in)

print(df_hr.head())

# Minutely:
data_type = "historical_intraday"

start_in = '2025-08-02 09:30'
end_in = '2025-09-21 16:00'

ticker = 'SPY'
interval = '1m'

#### I NEED TO GET TS_COL OUT OF THE RETURN FUNCTION HERE
df_min = get_df(provider, data_type, exchange, ticker, interval, start_in, end_in)

print(df_min.head())

# Streaming:
data_type = "streaming"

start_in = '2025-08-02 09:30'
end_in = '2025-09-21 16:00'

ticker = 'SPY'
interval = None

#### I NEED TO GET TS_COL OUT OF THE RETURN FUNCTION HERE
df_stream = get_df(provider, data_type, exchange, ticker, interval, start_in, end_in)

print(df_stream.head())



###PLOTTING:


# # RAW STREAMING ALL:
# import pandas as pd
# import matplotlib.pyplot as plt
# import matplotlib.dates as mdates

# # Parse the date strings exactly (fast and strict)
# df = df_stream.copy()
# df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d %H:%M:%S.%f", errors='coerce')

# # Helper to clean one series: ensure numeric y and drop rows with missing x/y
# def xy(df, ycol):
#     tmp = df[['date', ycol]].copy()
#     tmp[ycol] = pd.to_numeric(tmp[ycol], errors='coerce')
#     tmp = tmp.dropna(subset=['date', ycol])
#     return tmp['date'].to_numpy(), tmp[ycol].to_numpy()

# x_spot, y_spot = xy(df, 'price')
# x_bid,  y_bid  = xy(df, 'bid_price')
# x_ask,  y_ask  = xy(df, 'ask_price')

# plt.figure(figsize=(12, 6))
# plt.scatter(x_spot, y_spot, s=12, alpha=0.9, color='black', label='Spot Price')
# plt.scatter(x_bid,  y_bid,  s=12, alpha=0.9, color='blue',  label='Bid Price')
# plt.scatter(x_ask,  y_ask,  s=12, alpha=0.9, color='green', label='Ask Price')  # green label fixed

# plt.xlabel('Date')
# plt.ylabel('Price')
# plt.title('Price vs Date')
# plt.legend()
# plt.grid(True, linestyle='--', linewidth=0.5, alpha=0.6)

# # Nice date formatting
# ax = plt.gca()
# locator = mdates.AutoDateLocator()
# ax.xaxis.set_major_locator(locator)
# ax.xaxis.set_major_formatter(mdates.AutoDateFormatter(locator))
# plt.gcf().autofmt_xdate()

# plt.tight_layout()
# plt.show()


# # STREAMING PLUS SINGLE DAY CANDLE
# import pandas as pd
# import matplotlib.pyplot as plt
# import matplotlib.dates as mdates
# from matplotlib.patches import Rectangle
# import numpy as np

# def plot_prices_with_candle(df_stream, df_day):
#     df = df_stream.copy()

#     # 1) Parse intraday timestamps
#     df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d %H:%M:%S.%f", errors='coerce')

#     # 2) Coerce price columns to numeric
#     for col in ['price', 'bid_price', 'ask_price']:
#         if col in df.columns:
#             df[col] = pd.to_numeric(df[col], errors='coerce')

#     # Drop rows missing x or all y
#     df = df.dropna(subset=['date'])
#     if df.empty:
#         raise ValueError("No valid datetimes in df_stream['date'].")

#     # 3) Extract and coerce OHLC for the candle
#     row = df_day.iloc[1]
#     o = pd.to_numeric(row['open'], errors='coerce')
#     c = pd.to_numeric(row['close'], errors='coerce')
#     h = pd.to_numeric(row['high'], errors='coerce')
#     l = pd.to_numeric(row['low'],  errors='coerce')
#     if np.isnan([o, c, h, l]).any():
#         raise ValueError(f"OHLC contains non-numeric values after coercion: "
#                          f"open={row['open']} close={row['close']} high={row['high']} low={row['low']}")

#     # 4) Define candle span for the session (same day as first df point)
#     day0  = df['date'].iloc[0].normalize()
#     start = day0 + pd.Timedelta(hours=9, minutes=30)
#     end   = day0 + pd.Timedelta(hours=16)

#     # 5) Plot
#     fig, ax = plt.subplots(figsize=(12, 6))

#     # Candle (behind)
#     x0 = mdates.date2num(start)
#     x1 = mdates.date2num(end)
#     width = x1 - x0
#     body_low, body_high = (min(o, c), max(o, c))
#     color = 'green' if c >= o else 'red'

#     rect = Rectangle(
#         (x0, body_low),
#         width,
#         body_high - body_low,
#         facecolor=color,
#         edgecolor=color,
#         alpha=0.15,
#         zorder=0
#     )
#     ax.add_patch(rect)

#     x_mid = (x0 + x1) / 2.0
#     ax.vlines(x_mid, l, h, color=color, linewidth=1.0, alpha=0.6, zorder=0.25)

#     # Scatter series (drop NaNs per series)
#     for col, label, c_ in [
#         ('price',     'Spot Price', 'black'),
#         ('bid_price', 'Bid Price',  'blue'),
#         ('ask_price', 'Ask Price',  'green'),
#     ]:
#         if col in df.columns:
#             t = df[['date', col]].dropna()
#             if not t.empty:
#                 ax.scatter(t['date'].to_numpy(), t[col].to_numpy(), s=12, alpha=0.9, color=c_, label=label)

#     # Labels/formatting
#     ax.set_xlabel('Date')
#     ax.set_ylabel('Price')
#     ax.set_title('Price vs Date with Session Candle (9:30–16:00)')
#     ax.legend()
#     ax.grid(True, linestyle='--', linewidth=0.5, alpha=0.6)

#     # Make sure candle range is visible even if outside scatter range
#     ymin, ymax = ax.get_ylim()
#     ax.set_ylim(min(ymin, l), max(ymax, h))

#     # Date axis formatting
#     loc = mdates.AutoDateLocator()
#     ax.xaxis.set_major_locator(loc)
#     ax.xaxis.set_major_formatter(mdates.AutoDateFormatter(loc))
#     fig.autofmt_xdate()

#     plt.tight_layout()
#     plt.show()

# plot_prices_with_candle(df_stream, df_day)


# --- 1) Daily OHLC from df_daily.iloc[1] ---
row = df_day.iloc[1]
o_daily = row['open']
c_daily = row['close']
h_daily = row['high']
l_daily = row['low']

print("From Daily Candle:")
print(f"  Open:  {o_daily}")
print(f"  Close: {c_daily}")
print(f"  High:  {h_daily}")
print(f"  Low:   {l_daily}")

# --- 2) Intraday OHLC from df_stream (9:30–16:00) ---
df = df_stream.copy()
df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d %H:%M:%S.%f", errors='coerce')
df = df.dropna(subset=['date', 'price']).sort_values('date')

# Pick the trading day from the first timestamp (preserves tz if present)
first_ts = df['date'].iloc[0]
day = first_ts.normalize()   # midnight of that calendar day, same tz-awareness
start = day + pd.Timedelta(hours=9, minutes=30)
end   = day + pd.Timedelta(hours=16)

# Session slice
session = df[(df['date'] >= start) & (df['date'] <= end)]


o_intraday = session.iloc[(session['date'] - start).abs().argmin()]['price']
c_intraday = session.iloc[(session['date'] - end).abs().argmin()]['price']
h_intraday = session['price'].max()
l_intraday = session['price'].min()

print("\nFrom df_stream between 9:30–16:00:")
print(f"  Open:  {o_intraday}")
print(f"  Close: {c_intraday}")
print(f"  High:  {h_intraday}")
print(f"  Low:   {l_intraday}")
