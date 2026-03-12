import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
from typing import Optional

load_dotenv()

_SUPABASE_URL = os.getenv("SUPABASE_URL")
_SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not _SUPABASE_URL or not _SUPABASE_KEY:
    raise EnvironmentError("SUPABASE_URL and SUPABASE_KEY must be set in your .env file.")

_client: Optional[Client] = None


def _get_client() -> Client:
    """Returns a cached Supabase client, creating one if it doesn't exist."""
    global _client
    if _client is None:
        _client = create_client(_SUPABASE_URL, _SUPABASE_KEY)
    return _client


def fetch_table(table_name: str) -> pd.DataFrame:
    """Fetches all rows from a Supabase table and returns them as a DataFrame."""
    client = _get_client()
    rows = []
    batch_size = 1000
    start = 0

    while True:
        response = client.table(table_name).select("*").range(start, start + batch_size - 1).execute()
        if not response.data:
            break
        rows.extend(response.data)
        start += batch_size

    return pd.DataFrame(rows)


def upload_dataframe(df: pd.DataFrame, table_name: str) -> None:
    """Uploads a DataFrame to a Supabase table in batches using upsert."""
    client = _get_client()
    records = df.to_dict(orient="records")
    batch_size = 500

    for i in range(0, len(records), batch_size):
        batch = records[i: i + batch_size]
        client.table(table_name).upsert(batch).execute()
        print(f"Uploaded rows {i + 1}–{min(i + batch_size, len(records))}")