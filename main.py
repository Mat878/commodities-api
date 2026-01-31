import io
import matplotlib.pyplot as plt
from fastapi import FastAPI, Response
import pandas as pd
from sqlalchemy import create_engine

app = FastAPI()
engine = create_engine("postgresql+psycopg2://postgres:password@localhost:5432/commodities")

@app.get("/gold")
async def root():
    return {
        "price": "test",
        "symbol": "XAU/GBP"
    }
    """
    "symbol": "GOLD",
    "date": "2024-01-30",
    "open": 1932.10,
    "high": 1944.20,
    "low": 1928.50,
    "close": 1938.75,
    }
    """

@app.get("/gold/history")
def gold_history():
    df = pd.read_sql('SELECT "Date", "Close" FROM gold ORDER BY "Date"', engine)

    df["Date"] = pd.to_datetime(df["Date"])

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(df["Date"], df["Close"], color="gold")
    ax.set_title("Gold Price - 5 Year History")
    ax.set_xlabel("Date")
    ax.set_ylabel("Price (USD)")

    buf = io.BytesIO()
    plt.savefig(buf, format="png")
    plt.close(fig)
    buf.seek(0)

    return Response(content=buf.read(), media_type="image/png")

@app.get("/gold/analytics")
def gold_analytics():
    # Simple Moving Average (SMA): 7-day, 30-day, 200-day, SMA
    # Rolling volatility/some volatility measure
    # Daily Price Change
    # Daily Percent Return
    # 7- day sma
    # 52-week high and low
    # year to date return
    # maximum close price (alltie hgih)
    # Minimum Close Price (All‑Time Low)
    # average daily volatility

