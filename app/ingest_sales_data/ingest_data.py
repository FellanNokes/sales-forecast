from pathlib import Path
import pandas as pd

base_path = Path(__file__).resolve().parents[2]
file_path = base_path / "data" / "raw" / "coffee-shop-sales-revenue.csv"

df = pd.read_csv(file_path, sep="|")

print(df.head())


## terminal: python app/ingest_sales_data/ingest_data.py
