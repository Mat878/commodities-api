import pandas as pd
from sqlalchemy import create_engine

# sort gold price data
df = pd.read_csv("data/gold_prices.csv", sep=";")
df = df.drop(columns=["Volume"])
df["Date"] = pd.to_datetime(df["Date"], format="%Y.%m.%d %H:%M")
df = df[df["Date"] >= "2021-01-01"]

engine = create_engine("postgresql+psycopg2://postgres:password@localhost:5432/commodities")
df.to_sql("gold", engine, if_exists="replace", index=False)

