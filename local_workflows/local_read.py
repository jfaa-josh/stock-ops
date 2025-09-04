import pandas as pd
import matplotlib.pyplot as plt
import subprocess

from stockops.data.database.reader import ReadProcess

# Static
provider = "EODHD"
exchange = "US"

def run(provider, data_type, exchange, ticker, interval, start_date, end_date):
    reader = ReadProcess(provider, data_type, exchange)
    data = reader.read_sql(ticker, interval, start_date, end_date)
    return reader.get_df(data)

# Command to run inside WSL TO copy database
############################################
# cmd = [
#     "wsl", "docker", "cp",
#     "stockops-writer-service-1:/app/data/raw",
#     "./data/git_lfs"
# ]

# subprocess.run(cmd, check=True)
############################################



#### I THINK ITS PRETTY CLEAR HERE I NEED A FUNCTION TO RETURN AVAIL DATES/INTERVALS/TABLES IN HUMAN READABLE FORMAT!!!
###OK HERE IS THE PROBLEM WITHT HIS CODE, THAT I MIGHT CHOOSE TO PUT INTO READER AND I MIGHT NOT:
# I instance reader with a date range due to file names...so how do I first know exactly what I want from table stats??
# import sqlite3
# db_path = "path/to/your/database.db"

# # Connect to the database
# conn = sqlite3.connect(db_path)

# # Create a cursor
# cursor = conn.cursor()

# # Example: select first 10 rows from __interval_stats__
# cursor.execute("SELECT * FROM __interval_stats__ LIMIT 10;")
# rows = cursor.fetchall()

# for row in rows:
#     print(row)

# # Close when finished
# conn.close()
###################################################################################################################






# DAILY:
data_type = "historical_interday"

start_date = '2025-07-01'
end_date = '2025-08-21'

ticker = 'SPY'
interval = 'd'

#### I NEED TO GET TS_COL OUT OF THE RETURN FUNCTION HERE
df_day = run(provider, data_type, exchange, ticker, interval, start_date, end_date)

# HOURLY:
data_type = "historical_intraday"

start_date = '2025-07-01 09:30'
end_date = '2025-09-21 16:00'

ticker = 'SPY'
interval = '1h'

df_hr = run(provider, data_type, exchange, ticker, interval, start_date, end_date)

print(df_hr.head())

# # Minutely:
# data_type = "historical_intraday"

# start_date = '2025-08-02 09:30'
# end_date = '2025-09-21 16:00'

# ticker = 'SPY'
# interval = '1m'

# df_min = run(provider, data_type, exchange, ticker, interval, start_date, end_date)

# print(df_min.head())

# Streaming:
data_type = "streaming"

start_date = '2025-08-02 09:30'
end_date = '2025-09-21 16:00'

ticker = 'SPY'
interval = None

df_stream = run(provider, data_type, exchange, ticker, interval, start_date, end_date)

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

# Pick the trading day from the first timestamp (preserves tz if present)
first_ts = df.index[0]
day = first_ts.normalize()   # midnight of that calendar day, same tz-awareness
start = day + pd.Timedelta(hours=9, minutes=30)
end   = day + pd.Timedelta(hours=16)

# Session slice
session = df[(df.index >= start) & (df.index <= end)]


o_intraday = session.iloc[(session['date'] - start).abs().argmin()]['price']
c_intraday = session.iloc[(session['date'] - end).abs().argmin()]['price']
h_intraday = session['price'].max()
l_intraday = session['price'].min()

print("\nFrom df_stream between 9:30–16:00:")
print(f"  Open:  {o_intraday}")
print(f"  Close: {c_intraday}")
print(f"  High:  {h_intraday}")
print(f"  Low:   {l_intraday}")
