from pathlib import Path
from fastapi import APIRouter
from fastapi.responses import FileResponse
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import create_engine, text
import pandas as pd
import matplotlib.pyplot as plt
from data_pipeline import web_scrape


router = APIRouter()
engine = create_engine(
    "postgresql+psycopg2://postgres:password@localhost:5432/commodities"
)


@router.get("/gold")
async def gold():
    """Return the latest gold price"""
    await run_in_threadpool(
        web_scrape.main
    )  # prevent web scrape from blocking the event loop
    with engine.connect() as conn:
        row = conn.execute(
            text('SELECT * FROM gold ORDER BY "Date" DESC LIMIT 1;')
        ).fetchone()
    return {
        "symbol": "XAU/USD",
        "date": row[0].date(),
        "open": round(row[1], 2),
        "high": round(row[2], 2),
        "low": round(row[3], 2),
        "close": None,
    }


@router.get("/gold/history")
async def gold_history():
    """Return a 5‑year gold price chart as an image"""
    web_scrape.main()
    df = pd.read_sql('SELECT "Date", "Close" FROM gold ORDER BY "Date"', engine)
    df["Date"] = pd.to_datetime(df["Date"])

    plt.figure(figsize=(20, 8))
    plt.plot(df["Date"], df["Close"], color="gold")
    plt.title("5 Year Gold Price in USD/oz")
    plt.xlabel("Year")
    plt.ylabel("Price")
    plt.savefig("gold_history.png", bbox_inches="tight")

    image_path = Path("gold_history.png")
    return FileResponse(image_path)


@router.get("/gold/analytics")
async def gold_analytics():
    """Return moving averages and 52 week high/low analytics"""
    web_scrape.main()
    with engine.connect() as conn:
        sma_7 = conn.execute(
            text(
                """
            SELECT AVG("Close")
            FROM gold
            WHERE "Date" > CURRENT_DATE - INTERVAL '8 days' AND "Close" IS NOT NULL;
            """
            )
        ).scalar()

    with engine.connect() as conn:
        sma_30 = conn.execute(
            text(
                """
            SELECT AVG("Close")
            FROM gold
            WHERE "Date" > CURRENT_DATE - INTERVAL '31 days' AND "Close" IS NOT NULL;
            """
            )
        ).scalar()

    with engine.connect() as conn:
        sma_200 = conn.execute(
            text(
                """
            SELECT AVG("Close")
            FROM gold
            WHERE "Date" > CURRENT_DATE - INTERVAL '201 days' AND "Close" IS NOT NULL;
            """
            )
        ).scalar()

    with engine.connect() as conn:
        fifty_two_week_high = conn.execute(
            text(
                """
            SELECT MAX("High")
            FROM gold
            WHERE "Date" >= CURRENT_DATE - INTERVAL '365 days';
            """
            )
        ).scalar()

    with engine.connect() as conn:
        fifty_two_week_low = conn.execute(
            text(
                """
            SELECT MIN("Low")
            FROM gold
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
