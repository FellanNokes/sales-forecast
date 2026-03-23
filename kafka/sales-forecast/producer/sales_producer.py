import json
import os
import time
from datetime import date, timedelta
from datetime import datetime

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from supabase import create_client

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "sales-forecast"

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

FORECAST_DAYS = 7

# ---------------------------------------------------------------------------
# Stores
# ---------------------------------------------------------------------------

STORES = [
    {"store_id": 5, "store_location": "Lower Manhattan"},
    {"store_id": 8, "store_location": "Hell's Kitchen"},
    {"store_id": 3, "store_location": "Astoria"},
]

# ---------------------------------------------------------------------------
# Product catalogue
# ---------------------------------------------------------------------------

PRODUCTS = [
    (14, "Coffee", "Barista Espresso",      "Cappuccino",                    3.75),
    (15, "Coffee", "Barista Espresso",      "Cappuccino Lg",                 4.25),
    (16, "Coffee", "Barista Espresso",      "Espresso shot",                 3.00),
    (17, "Coffee", "Barista Espresso",      "Latte",                         3.75),
    (18, "Coffee", "Barista Espresso",      "Latte Rg",                      4.25),
    (19, "Coffee", "Barista Espresso",      "Ouro Brasileiro shot",          3.00),
    (20, "Coffee", "Drip coffee",           "Our Old Time Diner Blend Lg",   3.00),
    (21, "Coffee", "Drip coffee",           "Our Old Time Diner Blend Rg",   2.50),
    (22, "Coffee", "Drip coffee",           "Our Old Time Diner Blend Sm",   2.00),
    (23, "Coffee", "Gourmet brewed coffee", "Columbian Medium Roast Lg",     3.00),
    (24, "Coffee", "Gourmet brewed coffee", "Columbian Medium Roast Rg",     2.50),
    (25, "Coffee", "Gourmet brewed coffee", "Columbian Medium Roast Sm",     2.00),
    (26, "Coffee", "Gourmet brewed coffee", "Ethiopia Lg",                   3.50),
    (27, "Coffee", "Gourmet brewed coffee", "Ethiopia Rg",                   3.00),
    (28, "Coffee", "Gourmet brewed coffee", "Ethiopia Sm",                   2.20),
    (29, "Coffee", "Organic brewed coffee", "Brazilian Lg",                  3.50),
    (30, "Coffee", "Organic brewed coffee", "Brazilian Rg",                  3.00),
    (31, "Coffee", "Organic brewed coffee", "Brazilian Sm",                  2.20),
    (32, "Coffee", "Premium brewed coffee", "Jamaican Coffee River Lg",      3.75),
    (33, "Coffee", "Premium brewed coffee", "Jamaican Coffee River Rg",      3.10),
    (34, "Coffee", "Premium brewed coffee", "Jamaican Coffee River Sm",      2.45),
    (64, "Tea", "Brewed Black tea",  "Earl Grey Lg",              3.00),
    (65, "Tea", "Brewed Black tea",  "Earl Grey Rg",              2.50),
    (66, "Tea", "Brewed Black tea",  "English Breakfast Lg",      3.00),
    (67, "Tea", "Brewed Black tea",  "English Breakfast Rg",      2.50),
    (68, "Tea", "Brewed Chai tea",   "Morning Sunrise Chai Lg",   4.00),
    (69, "Tea", "Brewed Chai tea",   "Morning Sunrise Chai Rg",   2.50),
    (70, "Tea", "Brewed Chai tea",   "Spicy Eye Opener Chai Lg",  3.10),
    (72, "Tea", "Brewed Chai tea",   "Traditional Blend Chai Lg", 3.00),
    (74, "Tea", "Brewed Green tea",  "Serenity Green Tea Lg",     3.00),
    (76, "Tea", "Brewed herbal tea", "Lemon Grass Lg",            3.00),
    (78, "Tea", "Brewed herbal tea", "Peppermint Lg",             3.00),
    (45, "Drinking Chocolate", "Hot chocolate", "Dark chocolate Lg",               4.50),
    (46, "Drinking Chocolate", "Hot chocolate", "Dark chocolate Rg",               3.50),
    (47, "Drinking Chocolate", "Hot chocolate", "Sustainably Grown Organic Lg",    4.75),
    (48, "Drinking Chocolate", "Hot chocolate", "Sustainably Grown Organic Rg",    3.75),
    (0,  "Bakery", "Biscotti", "Chocolate Chip Biscotti", 3.50),
    (1,  "Bakery", "Biscotti", "Ginger Biscotti",         3.50),
    (3,  "Bakery", "Pastry",   "Almond Croissant",        3.75),
    (4,  "Bakery", "Pastry",   "Chocolate Croissant",     3.75),
    (5,  "Bakery", "Pastry",   "Croissant",               3.50),
    (6,  "Bakery", "Scone",    "Cranberry Scone",         3.25),
    (9,  "Bakery", "Scone",    "Oatmeal Scone",           3.00),
    (49, "Flavours", "Regular syrup",    "Carmel syrup",             0.80),
    (51, "Flavours", "Regular syrup",    "Hazelnut syrup",           0.80),
    (52, "Flavours", "Sugar free syrup", "Sugar Free Vanilla syrup", 0.80),
    (36, "Coffee beans", "Gourmet Beans",     "Columbian Medium Roast",   15.00),
    (38, "Coffee beans", "Gourmet Beans",     "Ethiopia",                 21.00),
    (41, "Coffee beans", "Organic Beans",     "Brazilian - Organic",      18.00),
    (40, "Coffee beans", "House blend Beans", "Our Old Time Diner Blend", 18.00),
    (12, "Branded",            "Housewares",         "I Need My Bean! Diner mug", 12.00),
    (62, "Packaged Chocolate", "Drinking Chocolate", "Dark chocolate",              6.40),
    (63, "Packaged Chocolate", "Organic Chocolate",  "Sustainably Grown Organic",   7.60),
]

PRODUCT_LOOKUP = {p[0]: p[1:] for p in PRODUCTS}

# ---------------------------------------------------------------------------
# Weather → sales multipliers
# ---------------------------------------------------------------------------

WEATHER_CODE_MAP = {
    0: "clear",  1: "clear",  2: "cloudy",  3: "cloudy",
    45: "foggy", 48: "foggy",
    51: "drizzle", 53: "drizzle", 55: "drizzle",
    56: "freezing_drizzle", 57: "freezing_drizzle",
    61: "rain",  63: "rain",  65: "rain",
    66: "freezing_rain", 67: "freezing_rain",
    71: "snow",  73: "snow",  75: "snow",  77: "heavy_snow",
    80: "rain_showers", 81: "rain_showers", 82: "rain_showers",
    85: "snow_showers", 86: "snow_showers",
    95: "thunderstorm", 96: "thunderstorm", 99: "thunderstorm",
}

TEMP_MULTIPLIER = {
    "warm": 1.20, "mild": 1.00, "cold": 0.68, "freezing": 0.59,
}

CONDITION_MULTIPLIER = {
    "clear": 1.15, "cloudy": 1.00, "drizzle": 0.97, "foggy": 0.92,
    "rain": 0.90, "rain_showers": 0.88, "freezing_drizzle": 0.78,
    "freezing_rain": 0.70, "snow": 0.75, "snow_showers": 0.73,
    "heavy_snow": 0.65, "thunderstorm": 0.60,
}

CATEGORY_WEATHER_BOOST = {
    "freezing": {"Drinking Chocolate": 2.0, "Bakery": 1.3, "Tea": 1.2},
    "cold":     {"Drinking Chocolate": 1.5, "Bakery": 1.15, "Tea": 1.1},
    "mild":     {},
    "warm":     {"Coffee": 1.1, "Tea": 0.9, "Drinking Chocolate": 0.7},
}

HOURLY_WEIGHTS = {
    6: 4594,  7: 13428, 8: 17654, 9: 17764, 10: 18545,
    11: 9766, 12: 8708, 13: 8714, 14: 8933,  15: 8979,
    16: 9093, 17: 8745, 18: 7498, 19: 6092,  20: 603,
}
HOURS = list(HOURLY_WEIGHTS.keys())
HOUR_PROBS = np.array(list(HOURLY_WEIGHTS.values()), dtype=float)
HOUR_PROBS /= HOUR_PROBS.sum()

BASE_DAILY_TRANSACTIONS = 275

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def categorize_temperature(temp: float) -> str:
    if temp < 0:    return "freezing"
    elif temp < 9:  return "cold"
    elif temp < 17: return "mild"
    return "warm"


def fetch_weather_forecast() -> list[dict]:
    """Fetch weather_forecast rows for the coming FORECAST_DAYS from Supabase."""
    client = create_client(SUPABASE_URL, SUPABASE_KEY)
    start = date.today().isoformat()
    end   = (date.today() + timedelta(days=FORECAST_DAYS - 1)).isoformat()

    response = (
        client.table("weather_forecast")
        .select("*")
        .gte("date", start)
        .lte("date", end)
        .execute()
    )
    return response.data


def _build_product_weights(temp_category: str):
    boosts = CATEGORY_WEATHER_BOOST.get(temp_category, {})
    base = {
        "Coffee": 4.5, "Tea": 2.5, "Bakery": 1.5,
        "Drinking Chocolate": 0.8, "Flavours": 0.5,
        "Coffee beans": 0.15, "Branded": 0.05, "Packaged Chocolate": 0.05,
    }
    ids, weights = [], []
    for pid, (cat, *_) in PRODUCT_LOOKUP.items():
        w = base.get(cat, 0.1) * boosts.get(cat, 1.0)
        ids.append(pid)
        weights.append(w)
    weights = np.array(weights)
    return ids, weights / weights.sum()


def generate_transactions(store: dict, weather_row: dict) -> list[dict]:
    temp      = weather_row["temperature_mean"]
    code      = weather_row["weather_code"]
    temp_cat  = categorize_temperature(temp)
    condition = WEATHER_CODE_MAP.get(code, "cloudy")

    temp_mult = TEMP_MULTIPLIER.get(temp_cat, 1.0)
    cond_mult = CONDITION_MULTIPLIER.get(condition, 1.0)
    n = int(BASE_DAILY_TRANSACTIONS * temp_mult * cond_mult)
    n = max(1, int(np.random.normal(n, n * 0.10)))

    product_ids, product_weights = _build_product_weights(temp_cat)
    chosen_products = np.random.choice(product_ids, size=n, p=product_weights)
    chosen_hours    = np.random.choice(HOURS, size=n, p=HOUR_PROBS)

    transactions = []
    for pid, hour in zip(chosen_products, chosen_hours):
        cat, ptype, detail, price = PRODUCT_LOOKUP[pid]
        import random
        qty = int(np.random.choice([1, 2, 3, 4], p=[0.72, 0.20, 0.06, 0.02]))
        transactions.append({
            "transaction_date":  weather_row["date"],
            "transaction_time":  f"{hour:02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}",
            "transaction_qty":   qty,
            "store_id":          store["store_id"],
            "store_location":    store["store_location"],
            "product_id":        int(pid),
            "unit_price":        price,
            "product_category":  cat,
            "product_type":      ptype,
            "product_detail":    detail,
            "weather_condition": condition,
            "temp_category":     temp_cat,
            "temperature_mean":  temp,
            "is_synthetic":      True,
            "generated_at":      datetime.utcnow().isoformat(),
        })
    return transactions


# ---------------------------------------------------------------------------
# Kafka producer
# ---------------------------------------------------------------------------

def create_producer(retries=10, delay=5):
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"Ansluten till Kafka på {KAFKA_BROKER}")
            return producer
        except Exception as e:
            print(f"Försök {attempt + 1}/{retries} misslyckades: {e}")
            print(f"Väntar {delay}s innan nästa försök...")
            time.sleep(delay)
    raise Exception("Kunde inte ansluta till Kafka efter flera försök")


def main():
    producer = create_producer()
    print(f"Skickar till topic: {TOPIC}")

    while True:
        print(f"\n[{datetime.now()}] Genererar försäljningsdata...")
        try:
            weather_rows = fetch_weather_forecast()

            if not weather_rows:
                print("Ingen väderdata hittades i Supabase — väntar...")
            else:
                # Group weather by store_location for easy lookup
                weather_by_store = {}
                for row in weather_rows:
                    loc = (
                        row["store_location"]
                        .replace(", New York, United States", "")
                        .replace(", United States", "")
                        .strip()
                    )
                    weather_by_store.setdefault(loc, []).append(row)

                total = 0
                for store in STORES:
                    rows = weather_by_store.get(store["store_location"], [])
                    for weather_row in rows:
                        transactions = generate_transactions(store, weather_row)
                        for txn in transactions:
                            producer.send(TOPIC, value=txn)
                        total += len(transactions)
                        print(f"  {store['store_location']} | {weather_row['date']} → {len(transactions)} transaktioner")

                producer.flush()
                print(f"Skickade {total} meddelanden till Kafka!")

        except Exception as e:
            print(f"Fel: {e}")

        print(f"Väntar 1 dag innan nästa körning...")
        time.sleep(86400)


if __name__ == "__main__":
    main()
