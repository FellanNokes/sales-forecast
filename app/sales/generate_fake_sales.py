"""
generate_fake_sales.py

Generates realistic but intentionally messy fake sales transactions
for the coming N days, modulated by weather forecast data fetched
from the `weather_forecast` Supabase table.

~5% of rows contain data quality issues to be caught by process_sales_data.py:
    - Missing values (transaction_id, date, time, store_id, store_location,
      unit_price, transaction_qty)
    - Negative unit_price or transaction_qty
    - Non-positive transaction_qty (zero)
    - Unrealistically high unit_price (>100)
    - Malformed date/time strings
    - Duplicate transaction_ids

# TODO: Remove weather categorization logic from this file once it is
# handled upstream (e.g. a dedicated pipeline step that writes to a
# weather_features table). At that point, fetch from weather_features
# directly instead of weather_forecast + add_weather_features().
"""

import argparse
import random
from datetime import date, timedelta
from typing import Optional

import numpy as np
import pandas as pd

from app.utility.supabase_functions import fetch_table
from app.weather.weather_features import add_weather_features

# ---------------------------------------------------------------------------
# Store catalogue
# ---------------------------------------------------------------------------

STORES = [
    {"store_id": 5, "store_location": "Lower Manhattan"},
    {"store_id": 8, "store_location": "Hell's Kitchen"},
    {"store_id": 3, "store_location": "Astoria"},
]

# ---------------------------------------------------------------------------
# Product catalogue  (product_id, category, type, detail, unit_price)
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
# Mess injection (~5% of rows)
# ---------------------------------------------------------------------------

MESS_TYPES = [
    "missing_transaction_id",
    "missing_date",
    "missing_time",
    "missing_store_id",
    "missing_store_location",
    "missing_unit_price",
    "negative_unit_price",
    "missing_qty",
    "negative_qty",
    "zero_qty",
    "unrealistic_price",
    "malformed_date",
    "malformed_time",
    "duplicate_id",
]

MESS_RATE = 0.05


def _inject_mess(row: dict, mess_type: str, duplicate_id: Optional[int] = None) -> dict:
    """Inject a single data quality issue into a row."""
    row = row.copy()
    if mess_type == "missing_transaction_id":
        row["transaction_id"] = None
    elif mess_type == "missing_date":
        row["transaction_date"] = None
    elif mess_type == "missing_time":
        row["transaction_time"] = None
    elif mess_type == "missing_store_id":
        row["store_id"] = None
    elif mess_type == "missing_store_location":
        row["store_location"] = None
    elif mess_type == "missing_unit_price":
        row["unit_price"] = None
    elif mess_type == "negative_unit_price":
        row["unit_price"] = -abs(row["unit_price"])
    elif mess_type == "missing_qty":
        row["transaction_qty"] = None
    elif mess_type == "negative_qty":
        row["transaction_qty"] = -abs(row["transaction_qty"])
    elif mess_type == "zero_qty":
        row["transaction_qty"] = 0
    elif mess_type == "unrealistic_price":
        row["unit_price"] = round(random.uniform(150.0, 999.99), 2)
    elif mess_type == "malformed_date":
        row["transaction_date"] = random.choice(["2026-99-99", "not-a-date", "32/13/2026", ""])
    elif mess_type == "malformed_time":
        row["transaction_time"] = random.choice(["99:99:99", "not-a-time", "25:00:00", ""])
    elif mess_type == "duplicate_id" and duplicate_id is not None:
        row["transaction_id"] = duplicate_id
    return row


# ---------------------------------------------------------------------------
# Weather fetching from Supabase
# ---------------------------------------------------------------------------

def fetch_weather_for_dates(start: date, end: date) -> pd.DataFrame:
    """
    Fetches weather_forecast rows from Supabase for the given date range,
    then applies add_weather_features() to categorize temp and conditions.

    # TODO: Remove add_weather_features() call here once weather categorization
    # is handled upstream and stored directly in a weather_features table.
    # At that point, replace fetch_table("weather_forecast") with
    # fetch_table("weather_features") and remove the add_weather_features() call.
    """
    print("Fetching weather_forecast from Supabase...")
    df = fetch_table("weather_forecast")

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df[(df["date"] >= start) & (df["date"] <= end)].copy()

    if df.empty:
        raise ValueError(
            f"No weather_forecast rows found between {start} and {end}. "
            "Make sure the Kafka producer has populated the table."
        )

    df["store_location"] = (
        df["store_location"]
        .str.replace(r",\s*(New York,\s*)?United States$", "", regex=True)
        .str.strip()
    )

    # TODO: Remove this block when weather categorization is done upstream.
    df = add_weather_features(df)

    return df


# ---------------------------------------------------------------------------
# Transaction generator
# ---------------------------------------------------------------------------

def _build_product_weights(temp_category: str) -> tuple[list, np.ndarray]:
    boosts = CATEGORY_WEATHER_BOOST.get(temp_category, {})
    base_weights = {
        "Coffee": 4.5, "Tea": 2.5, "Bakery": 1.5,
        "Drinking Chocolate": 0.8, "Flavours": 0.5,
        "Coffee beans": 0.15, "Branded": 0.05, "Packaged Chocolate": 0.05,
    }
    ids, weights = [], []
    for pid, (cat, *_) in PRODUCT_LOOKUP.items():
        w = base_weights.get(cat, 0.1) * boosts.get(cat, 1.0)
        ids.append(pid)
        weights.append(w)
    weights = np.array(weights)
    return ids, weights / weights.sum()


def _generate_transactions_for_day(
        store: dict,
        forecast_row: pd.Series,
        base_transaction_id: int,
) -> list[dict]:
    temp_mult = TEMP_MULTIPLIER.get(forecast_row["temp_category"], 1.0)
    cond_mult = CONDITION_MULTIPLIER.get(forecast_row["weather_condition"], 1.0)
    n = int(BASE_DAILY_TRANSACTIONS * temp_mult * cond_mult)
    n = max(1, int(np.random.normal(n, n * 0.10)))

    product_ids, product_weights = _build_product_weights(forecast_row["temp_category"])
    chosen_products = np.random.choice(product_ids, size=n, p=product_weights)
    chosen_hours    = np.random.choice(HOURS, size=n, p=HOUR_PROBS)

    transactions = []
    seen_ids = []

    for i, (pid, hour) in enumerate(zip(chosen_products, chosen_hours)):
        cat, ptype, detail, price = PRODUCT_LOOKUP[pid]
        qty = int(np.random.choice([1, 2, 3, 4], p=[0.72, 0.20, 0.06, 0.02]))
        tid = base_transaction_id + i

        row = {
            "transaction_id":   tid,
            "transaction_date": str(forecast_row["date"]),
            "transaction_time": f"{hour:02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}",
            "transaction_qty":  qty,
            "store_id":         store["store_id"],
            "store_location":   store["store_location"],
            "product_id":       pid,
            "unit_price":       price,
            "product_category": cat,
            "product_type":     ptype,
            "product_detail":   detail,
            "is_synthetic":     True,
        }

        # Inject mess into ~5% of rows
        if random.random() < MESS_RATE:
            mess_type = random.choice(MESS_TYPES)
            duplicate_id = random.choice(seen_ids) if seen_ids and mess_type == "duplicate_id" else None
            row = _inject_mess(row, mess_type, duplicate_id)

        seen_ids.append(tid)
        transactions.append(row)

    return transactions


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def generate_fake_sales(
        days: int = 7,
        start_date: Optional[date] = None,
        output_path: Optional[str] = None,
        starting_transaction_id: int = 200_000,
        random_seed: Optional[int] = 42,
) -> pd.DataFrame:
    """
    Generate messy fake sales data for the coming N days, weather-adjusted.
    ~5% of rows contain data quality issues for process_sales_data.py to catch.

    Args:
        days:                    Number of days to generate.
        start_date:              First date; defaults to today.
        output_path:             If set, saves result as CSV here.
        starting_transaction_id: Transaction IDs start from this number.
        random_seed:             Set for reproducibility; None for random.

    Returns:
        DataFrame with messy raw sales data.
    """
    if random_seed is not None:
        random.seed(random_seed)
        np.random.seed(random_seed)

    if start_date is None:
        start_date = date.today()
    end_date = start_date + timedelta(days=days - 1)

    weather_df = fetch_weather_for_dates(start_date, end_date)

    print(f"Generating transactions for {days} days across {len(STORES)} stores...")
    all_transactions = []
    tid = starting_transaction_id

    for store in STORES:
        store_weather = weather_df[weather_df["store_location"] == store["store_location"]]
        for _, row in store_weather.iterrows():
            day_txns = _generate_transactions_for_day(store, row, base_transaction_id=tid)
            all_transactions.extend(day_txns)
            tid += len(day_txns)
            print(
                f"  {store['store_location']} | {row['date']} | "
                f"{row['weather_condition']} {row['temp_category']} "
                f"({row['temperature_mean']:.1f}°C) → {len(day_txns)} transactions"
            )

    df = pd.DataFrame(all_transactions)
    df = df.sort_values(["transaction_date", "store_location", "transaction_time"]).reset_index(drop=True)

    messy_count = df.isnull().any(axis=1).sum()
    print(f"\nGenerated {len(df):,} transactions (~{messy_count} messy rows, {messy_count/len(df)*100:.1f}%)")

    if output_path:
        df.to_csv(output_path, index=False)
        print(f"Saved to {output_path}")

    return df


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate messy weather-adjusted fake coffee shop sales.")
    parser.add_argument("--days",   type=int, default=7,    help="Number of days to generate (default: 7)")
    parser.add_argument("--output", type=str, default=None, help="Output CSV path (optional)")
    parser.add_argument("--seed",   type=int, default=42,   help="Random seed (default: 42)")
    args = parser.parse_args()

    df = generate_fake_sales(days=args.days, output_path=args.output, random_seed=args.seed)
    print(df.head(10).to_string(index=False))