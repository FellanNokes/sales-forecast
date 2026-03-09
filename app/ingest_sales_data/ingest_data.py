from pathlib import Path
import pandas as pd

base_path = Path(__file__).resolve().parents[2]
file_path = base_path / "data" / "raw" / "coffee-shop-sales-revenue.csv"

df = pd.read_csv(file_path, sep="|")

print(df.head())


# terminal: python app/ingest_sales_data/ingest_data.py


# validate data

base_path = Path(__file__).resolve().parents[2]
file_path = base_path / "data" / "raw" / "coffee-shop-sales-revenue.csv"

df = pd.read_csv(file_path, sep="|")

print("First rows:")
print(df.head())

print("\nDataset info:")
print(df.info())

print("\nMissing values:")
print(df.isnull().sum())


# Remove unnecessary columns
df = df.drop(columns=[
    "transaction_id",
    "product_id",
])


print("\nColumns after cleaning:")
print(df.columns)
