import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
from sqlalchemy import create_engine, text

con = psycopg2.connect(
   database="postgres",
    user='postgres',
    password='password',
    host='localhost',
    port= '5432'
)

con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = con.cursor()

cursor.execute("DROP DATABASE IF EXISTS commodities;")
cursor.execute("CREATE DATABASE commodities;")

# gold
df = pd.read_csv("data/gold_prices.csv", sep=";")
df = df.drop(columns=["Volume"])
df["Date"] = pd.to_datetime(df["Date"], format="%Y.%m.%d %H:%M")
df = df[df["Date"] >= "2021-01-01"]

engine = create_engine("postgresql+psycopg2://postgres:password@localhost:5432/commodities")
df.to_sql("gold", engine, if_exists="replace", index=False)

# Add primary key to database table
with engine.connect() as conn:
    conn.execute(text('ALTER TABLE gold ADD PRIMARY KEY ("Date");'))
    conn.commit()

# oil
df = pd.read_csv("data_pipeline/data/oil_prices.csv")
df["Date"] = pd.to_datetime(df["Date"])
df = df[["Date", "Open", "High", "Low", "Close"]]
engine = create_engine("postgresql+psycopg2://postgres:password@localhost:5432/commodities")
df.to_sql("oil", engine, if_exists="replace", index=False)

# Add primary key to database table
with engine.connect() as conn:
    conn.execute(text('ALTER TABLE oil ADD PRIMARY KEY ("Date");'))
    conn.commit()

cursor.close()
conn.close()


# gas
df = pd.read_csv("data_pipeline/data/gas_prices.csv")
df["Date"] = pd.to_datetime(df["Date"])
df = df[["Date", "Open", "High", "Low", "Close"]]
engine = create_engine("postgresql+psycopg2://postgres:password@localhost:5432/commodities")
df.to_sql("gas", engine, if_exists="replace", index=False)

# Add primary key to database table
with engine.connect() as conn:
    conn.execute(text('ALTER TABLE gas ADD PRIMARY KEY ("Date");'))
    conn.commit()

cursor.close()
conn.close()

# wheat
df = pd.read_csv("data_pipeline/data/wheat_prices.csv")
df["Date"] = pd.to_datetime(df["Date"])
df = df[["Date", "Open", "High", "Low", "Close"]]
engine = create_engine("postgresql+psycopg2://postgres:password@localhost:5432/commodities")
df.to_sql("wheat", engine, if_exists="replace", index=False)

# Add primary key to database table
with engine.connect() as conn:
    conn.execute(text('ALTER TABLE wheat ADD PRIMARY KEY ("Date");'))
    conn.commit()

cursor.close()
conn.close()

# silver
df = pd.read_csv("data_pipeline/data/silver_prices.csv")
df["Date"] = pd.to_datetime(df["Date"])
df = df[["Date", "Open", "High", "Low", "Close"]]
engine = create_engine("postgresql+psycopg2://postgres:password@localhost:5432/commodities")
df.to_sql("silver", engine, if_exists="replace", index=False)

# Add primary key to database table
with engine.connect() as conn:
    conn.execute(text('ALTER TABLE silver ADD PRIMARY KEY ("Date");'))
    conn.commit()

cursor.close()
conn.close()