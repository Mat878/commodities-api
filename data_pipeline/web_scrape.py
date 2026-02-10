from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from datetime import date, timedelta
from sqlalchemy import create_engine, text

engine = create_engine("postgresql+psycopg2://postgres:password@localhost:5432/commodities")

def get_dates():
    with engine.connect() as conn:
        start_date = conn.execute(text('SELECT MAX("Date") FROM gold')).scalar()

    start_date += timedelta(days=1)
    end_date = date.today()

    dates = []
    curr = start_date.date()

    while curr <= end_date:
        dates.append(curr)
        curr += timedelta(days=1)

    return dates


def create_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--window-size=1920,1080")

    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)


def scrape_data(d):
    url = "today/" if d == date.today() else d.strftime("%Y/%B/%#d/")

    driver = create_driver()
    driver.get("https://pricegold.net/" + url)

    row = driver.find_element(By.XPATH, "//table//tbody/tr[7]")
    cells = row.find_elements(By.TAG_NAME, "td")

    if url == "today/":
        open_price = float(cells[2].text.replace("$", "").replace(",", ""))
        high_price = float(cells[3].text.replace("$", "").replace(",", ""))
        low_price = float(cells[4].text.replace("$", "").replace(",", ""))
        close_price = None
    else:
        open_price = float(cells[1].text.replace("$", "").replace(",", ""))
        high_price = float(cells[2].text.replace("$", "").replace(",", ""))
        low_price = float(cells[3].text.replace("$", "").replace(",", ""))
        close_price = float(cells[4].text.replace("$", "").replace(",", ""))

    driver.quit()

    return {
        "date": d,
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "close": close_price
    }


def save(record):
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO gold ("Date", "Open", "High", "Low", "Close")
                VALUES (:d, :o, :h, :l, :c)"""),
            {
                "d": record["date"],
                "o": record["open"],
                "h": record["high"],
                "l": record["low"],
                "c": record["close"]
            })

def check_latest():
    with engine.connect() as conn:
        conn.execute(text('DELETE FROM gold WHERE "Close" IS NULL;'))


def run():
    check_latest()
    dates = get_dates()

    for d in dates:
        record = scrape_data(d)
        save(record)

if __name__ == "__main__":
    run()
