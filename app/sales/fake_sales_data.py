from pathlib import Path
import pandas as pd
import random

# paths
base = Path(__file__).resolve().parents[2]
input_file = base / "data/raw/coffee-shop-sales-revenue.csv"
output_file = base / "data/raw/fake_sales_march_april.csv"

# load original data
df = pd.read_csv(input_file, sep="|")

# create dates for March and April
dates = pd.date_range("2023-03-01", "2023-04-30")

fake_rows = []

# generate fake sales data
for d in dates:
    for _ in range(random.randint(40, 100)):
        row = df.sample(1).iloc[0].copy()

        row["transaction_date"] = d.strftime("%Y-%m-%d")
        row["transaction_qty"] = max(
            1, int(row["transaction_qty"] * random.uniform(0.8, 1.2)))
        row["unit_price"] = round(
            row["unit_price"] * random.uniform(0.95, 1.05), 2)
        row["total_sales"] = round(
            row["transaction_qty"] * row["unit_price"], 2)

        fake_rows.append(row)

fake_df = pd.DataFrame(fake_rows)

# add a few messy rows
messy = [
    {
        "transaction_id": 999001,
        "transaction_date": "2023-03-15",
        "transaction_time": "09:15:00",
        "transaction_qty": -5,
        "store_id": 1,
        "store_location": "Lower Manhattan",
        "product_id": 101,
        "unit_price": 4.50,
        "product_category": "Coffee",
        "product_type": " Latte ",
        "product_detail": "Regular",
        "total_sales": -22.50
    },
    {
        "transaction_id": 999002,
        "transaction_date": "2023-03-22",
        "transaction_time": "10:30:00",
        "transaction_qty": 2,
        "store_id": 2,
        "store_location": "Astoria",
        "product_id": 102,
        "unit_price": "",
        "product_category": "Coffee",
        "product_type": "latte",
        "product_detail": "Small",
        "total_sales": ""
    },
    {
        "transaction_id": 999003,
        "transaction_date": "2023-04-02",
        "transaction_time": "14:00:00",
        "transaction_qty": 100,
        "store_id": 3,
        "store_location": "Hell's Kitchen",
        "product_id": 103,
        "unit_price": 999.99,
        "product_category": "Coffee",
        "product_type": "LATTE",
        "product_detail": "Large",
        "total_sales": 99999
    }
]

messy_df = pd.DataFrame(messy)

final_df = pd.concat([fake_df, messy_df], ignore_index=True)

# save with same separator as original file
final_df.to_csv(output_file, sep="|", index=False)

print("Fake data created:", output_file)
print("Rows:", len(final_df))
