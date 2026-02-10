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

df = pd.read_csv("data/gold_prices.csv", sep=";")
df = df.drop(columns=["Volume"])
df["Date"] = pd.to_datetime(df["Date"], format="%Y.%m.%d %H:%M")
df = df[df["Date"] >= "2021-01-01"]

engine = create_engine("postgresql+psycopg2://postgres:password@localhost:5432/commodities")
df.to_sql("gold", engine, if_exists="replace", index=False)

# Add primary key to database table
with engine.connect() as conn:
    conn.execute(text('ALTER TABLE gold ADD PRIMARY KEY ("Date");'))
    conn.execute(text("""
        ALTER TABLE gold
        ALTER COLUMN "Open"  TYPE numeric(10,2),
        ALTER COLUMN "High"  TYPE numeric(10,2),
        ALTER COLUMN "Low"   TYPE numeric(10,2),
        ALTER COLUMN "Close" TYPE numeric(10,2);
    """))
    conn.commit()


cursor.close()
conn.close()