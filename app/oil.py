from pathlib import Path
from fastapi import APIRouter
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, text
import pandas as pd
import matplotlib.pyplot as plt
from data_pipeline import latest_data

router = APIRouter()
engine = create_engine(
    "postgresql+psycopg2://postgres:password@localhost:5432/commodities"
)


@router.get("/oil")
async def oil():
    """Return the latest oil price"""
    latest_data.main()
    with engine.connect() as conn:
        row = conn.execute(
            text('SELECT * FROM oil ORDER BY "Date" DESC LIMIT 1;')
        ).fetchone()
    return {
        "symbol": "BRN",
        "date": row[0].date(),
        "open": round(row[1], 2),
        "high": round(row[2], 2),
        "low": round(row[3], 2),
        "close": round(row[4], 2),
    }


@router.get("/oil/history")
async def oil_history():
    """Return a 5‑year gas price chart as an image"""
    latest_data.main()
    df = pd.read_sql('SELECT "Date", "Close" FROM oil ORDER BY "Date"', engine)
    df["Date"] = pd.to_datetime(df["Date"])

    plt.figure(figsize=(20, 8))
    plt.plot(df["Date"], df["Close"], color="black")
    plt.title("5 Year Oil Price in USD/barrel")
    plt.xlabel("Year")
    plt.ylabel("Price")
    plt.savefig("oil_history.png", bbox_inches="tight")

    image_path = Path("oil_history.png")
    return FileResponse(image_path)


@router.get("/oil/analytics")
async def oil_analytics():
    """Return moving averages and 52 week high/low analytics"""
    latest_data.main()
    with engine.connect() as conn:
        sma_7 = conn.execute(
            text(
                """
            SELECT AVG("Close")
            FROM oil
            WHERE "Date" > CURRENT_DATE - INTERVAL '7 days';
            """
            )
        ).scalar()

    with engine.connect() as conn:
        sma_30 = conn.execute(
            text(
                """
            SELECT AVG("Close")
            FROM oil
            WHERE "Date" > CURRENT_DATE - INTERVAL '30 days';
            """
            )
        ).scalar()

    with engine.connect() as conn:
        sma_200 = conn.execute(
            text(
                """
            SELECT AVG("Close")
            FROM oil
            WHERE "Date" > CURRENT_DATE - INTERVAL '200 days';
            """
            )
        ).scalar()

    with engine.connect() as conn:
        fifty_two_week_high = conn.execute(
            text(
                """
            SELECT MAX("High")
            FROM oil
            WHERE "Date" >= CURRENT_DATE - INTERVAL '365 days';
            """
            )
        ).scalar()

    with engine.connect() as conn:
        fifty_two_week_low = conn.execute(
            text(
                """
            SELECT MIN("Low")
            FROM oil
            WHERE "Date" >= CURRENT_DATE - INTERVAL '365 days';
            """
            )
        ).scalar()

    return {
        "sma_7": round(sma_7, 2),
        "sma_30": round(sma_30, 2),
        "sma_200": round(sma_200, 2),
        "fifty_two_week_high": round(fifty_two_week_high, 2),
        "fifty_two_week_low": round(fifty_two_week_low, 2),
    }
