from sqlalchemy import create_engine, text
import yfinance as yf
import pandas as pd

engine = create_engine(
    "postgresql+psycopg2://postgres:password@localhost:5432/commodities"
)


def update_commodity(table_name, ticker):
    """Obtain the latest data ensuring commodity tables are up to date"""
    with engine.connect() as conn:
        start_date = conn.execute(
            text(f'SELECT MAX("Date") FROM {table_name}')
        ).scalar()

    data = yf.Ticker(ticker)
    df = data.history(start=start_date, interval="1d")

    df = df.reset_index()
    df = df[["Date", "Open", "High", "Low", "Close"]]
    df["Date"] = df["Date"].dt.tz_localize(
        None
    )  # remove timezone information to allow processing
    df = df[df["Date"] > start_date]

    df.to_sql(table_name, engine, if_exists="append", index=False)


commodities = [("silver", "SI=F"), ("oil", "BZ=F"), ("gas", "NG=F"), ("wheat", "ZW=F")]


def main():
    for table, ticker in commodities:
        update_commodity(table, ticker)


if __name__ == "__main__":
    main()
