from pathlib import Path

from supabase import create_client, Client
import pandas as pd
from dotenv import load_dotenv
import os

base_path = Path(__file__).resolve().parents[2]
csv_path = base_path / "data" / "cleaned" / "coffee-shop-sales-revenue.csv"


def load_sales_data(csv_path: Path, table_name: str) -> list[dict]:
    # Read .env
    load_dotenv()

    # url
    url = os.getenv("SUPABASE_URL")
    # key
    key = os.getenv("SUPABASE_KEY")

    # connect to superbase project with Client
    supabase: Client = create_client(url, key)

    # read the cleaned sales data
    df = pd.read_csv(csv_path, sep=",")

    print(df.columns.tolist())
    print(df.head(1).to_dict(orient="records"))

    records = df.to_dict(orient="records")
    batch_size = 500
    for i in range(0, len(records), batch_size):
        batch = records[i: i + batch_size]
        supabase.table(table_name).upsert(batch).execute()
        print(f"Uploaded rows {i + 1}–{min(i + batch_size, len(records))}")

    return records


if __name__ == "__main__":
    result = load_sales_data(csv_path, "historic_sales")
    print(result)
