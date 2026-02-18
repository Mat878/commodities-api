from sqlalchemy import create_engine, text
import yfinance as yf
import pandas as pd

# oil
engine = create_engine("postgresql+psycopg2://postgres:password@localhost:5432/commodities")
with engine.connect() as conn:
    start_date = conn.execute(text('SELECT MAX("Date") FROM oil')).scalar()

oil = yf.Ticker("BZ=F")
df = oil.history(start=start_date, interval="1d")

df = df.reset_index()
df = df[["Date", "Open", "High", "Low", "Close"]]
df["Date"] = df["Date"].dt.tz_localize(None)
df = df[df["Date"] > start_date]

df.to_sql("oil", engine, if_exists="append", index=False)


# gas
with engine.connect() as conn:
    start_date = conn.execute(text('SELECT MAX("Date") FROM gas')).scalar()

oil = yf.Ticker("NG=F")
df = oil.history(start=start_date, interval="1d")

df = df.reset_index()
df = df[["Date", "Open", "High", "Low", "Close"]]
df["Date"] = df["Date"].dt.tz_localize(None)
df = df[df["Date"] > start_date]

df.to_sql("gas", engine, if_exists="append", index=False)

# wheat
with engine.connect() as conn:
    start_date = conn.execute(text('SELECT MAX("Date") FROM wheat')).scalar()

oil = yf.Ticker("ZW=F")
df = oil.history(start=start_date, interval="1d")

df = df.reset_index()
df = df[["Date", "Open", "High", "Low", "Close"]]
df["Date"] = df["Date"].dt.tz_localize(None)
df = df[df["Date"] > start_date]

df.to_sql("wheat", engine, if_exists="append", index=False)

# silver
with engine.connect() as conn:
    start_date = conn.execute(text('SELECT MAX("Date") FROM silver')).scalar()

oil = yf.Ticker("SI=F")
df = oil.history(start=start_date, interval="1d")

df = df.reset_index()
df = df[["Date", "Open", "High", "Low", "Close"]]
df["Date"] = df["Date"].dt.tz_localize(None)
df = df[df["Date"] > start_date]

df.to_sql("silver", engine, if_exists="append", index=False)