import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
from sqlalchemy import create_engine, text


def create_database():
    """Creates a database to store all the commodity tables"""
    con = psycopg2.connect(
        database="postgres",
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
    )
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = con.cursor()

    cursor.execute("DROP DATABASE IF EXISTS commodities;")
    cursor.execute("CREATE DATABASE commodities;")

    con.commit()
    cursor.close()
    con.close()


def load_commodity(
    csv_path, table_name, engine, date_format=None, drop_volume=False, date_filter=None
):
    """Load a commodity CSV, clean the data and write it to a SQL table"""
    df = pd.read_csv(csv_path, sep=";" if drop_volume else ",")

    if drop_volume:
        df = df.drop(columns=["Volume"])

    if date_format:
        df["Date"] = pd.to_datetime(df["Date"], format=date_format)
    else:
        df["Date"] = pd.to_datetime(df["Date"])

    if date_filter:
        df = df[df["Date"] >= date_filter]

    df = df[["Date", "Open", "High", "Low", "Close"]]

    df.to_sql(table_name, engine, if_exists="replace", index=False)

    # Add primary key
    with engine.connect() as conn:
        conn.execute(text(f'ALTER TABLE {table_name} ADD PRIMARY KEY ("Date");'))
        conn.commit()


def main():
    create_database()

    engine = create_engine(
        "postgresql+psycopg2://postgres:password@localhost:5432/commodities"
    )

    load_commodity(
        "data_pipeline/data/gold_prices.csv",
        "gold",
        engine,
        date_format="%Y.%m.%d %H:%M",
        drop_volume=True,
        date_filter="2021-01-01",
    )

    load_commodity("data_pipeline/data/silver_prices.csv", "silver", engine)
    load_commodity("data_pipeline/data/oil_prices.csv", "oil", engine)
    load_commodity("data_pipeline/data/gas_prices.csv", "gas", engine)
    load_commodity("data_pipeline/data/wheat_prices.csv", "wheat", engine)


if __name__ == "__main__":
    main()
