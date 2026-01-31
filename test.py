import yfinance as yf
import pandas as pd

gold = yf.Ticker("XAUUSD=X")
df = gold.history(start="2026-01-01", end="2026-01-02")
print(df)