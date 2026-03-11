from supabase import create_client, Client
import pandas as pd
from dotenv import load_dotenv
import os


def load_sales_data(csv_path: str, table_name: str) -> list[dict]:
    # Read .env
    load_dotenv()

    # url
    url = os.getenv("SUPABASE_URL")
    # key
    key = os.getenv("SUPABASE_KEY")

    # connect to superbase project with Client
    supabase: Client = create_client(url, key)

    # read the cleaned sales data
    df = pd.read_csv(csv_path, sep="|")

    records = df.to_dict(orient="records")
    response = supabase.table(table_name).insert(records).execute()

    return response.data


result = load_sales_data(csv_path, table_name)
