# Commodity Price API
A FastAPI service that provides daily and historical commodity price data (gold, silver, oil, gas, wheat) backed by a PostgreSQL database and an automated data pipeline. It includes 5 year historical charts, moving-average analytics and 52 week high/low insights.

## Dependencies
- Python
- FastAPI
- Uvicorn
- PostgreSQL
- SQLAlchemy
- psycopg2
- Pandas
- Matplotlib
- yfinance
- Selenium
- Google Chrome 

## Getting Started
1. Clone this repository to your local machine
2. Install the dependencies
3. In the data_pipeline folder run load_data.py
4. Start the FastAPI server: uvicorn main:app --reload

## API Endpoints
Gold:    /gold, /gold/history, /gold/analytics

Silver:  /silver, /silver/history, /silver/analytics

Oil:     /oil, /oil/history, /oil/analytics

Gas:     /gas, /gas/history, /gas/analytics

Wheat:   /wheat, /wheat/history, /wheat/analytics


