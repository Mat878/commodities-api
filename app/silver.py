from pathlib import Path
from fastapi import APIRouter
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, text
import pandas as pd
import matplotlib.pyplot as plt

router = APIRouter()
engine = create_engine("postgresql+psycopg2://postgres:password@localhost:5432/commodities")

@router.get("/silver")
async def silver():
    with engine.connect() as conn:
        row = conn.execute(text('SELECT * FROM silver ORDER BY "Date" DESC LIMIT 1;')).fetchone()
    return {
        "symbol": "SI",
        "date": row[0].date(),
        "open": round(row[1], 2),
        "high": round(row[2], 2),
        "low": round(row[3], 2),
        "close": round(row[4], 2),
    }

@router.get("/silver/history")
async def silver_history():
    df = pd.read_sql('SELECT "Date", "Close" FROM silver ORDER BY "Date"', engine)
    df["Date"] = pd.to_datetime(df["Date"])

    plt.figure(figsize=(20, 8))
    plt.plot(df["Date"], df["Close"], color="silver")
    plt.title("5 Year Silver Price in USD/oz")
    plt.xlabel("Year")
    plt.ylabel("Price")
    plt.savefig('silver_history.png', bbox_inches='tight')
        
    image_path = Path("silver_history.png")
    return FileResponse(image_path)

@router.get("/silver/analytics")
async def silver_analytics():
    with engine.connect() as conn:
        sma_7 = conn.execute(text("""
            SELECT AVG("Close")
            FROM silver
            WHERE "Date" > CURRENT_DATE - INTERVAL '7 days';
            """)).scalar()
    
    with engine.connect() as conn:
        sma_30 = conn.execute(text("""
            SELECT AVG("Close")
            FROM silver
            WHERE "Date" > CURRENT_DATE - INTERVAL '30 days';
            """)).scalar()
    
    with engine.connect() as conn:
        sma_200 = conn.execute(text("""
            SELECT AVG("Close")
            FROM silver
            WHERE "Date" > CURRENT_DATE - INTERVAL '200 days';
            """)).scalar()
    
    with engine.connect() as conn:
        fifty_two_week_high = conn.execute(text("""
            SELECT MAX("High")
            FROM silver
            WHERE "Date" >= CURRENT_DATE - INTERVAL '365 days';
            """)).scalar()

    with engine.connect() as conn:
        fifty_two_week_low = conn.execute(text("""
            SELECT MIN("Low")
            FROM silver
            WHERE "Date" >= CURRENT_DATE - INTERVAL '365 days';
            """)).scalar()

    return {
        "sma_7": round(sma_7, 2),
        "sma_30": round(sma_30, 2),
        "sma_200": round(sma_200, 2),
        "fifty_two_week_high": round(fifty_two_week_high, 2),
        "fifty_two_week_low": round(fifty_two_week_low, 2)
    }