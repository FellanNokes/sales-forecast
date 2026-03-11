import os
from pathlib import Path
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()
CLEAN_DATA_PATH = Path(__file__).resolve().parents[2] / "data" / "cleaned" / "weather_clean.csv"

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://your-project.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "your-anon-key")

TABLE_NAME = "historic_weather"


def get_supabase_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY) # creates supabase connection


def upload_weather(df: pd.DataFrame, client: Client) -> None:
    
    records = df.to_dict(orient="records") # returns list of rows to be able to be mapped by supabase client

    # splits the list in batches to avoid hitting supabase size limits
    batch_size = 500
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        client.table(TABLE_NAME).upsert(batch).execute()
        print(f"Uploaded rows {i + 1}–{min(i + batch_size, len(records))}")


if __name__ == "__main__":
    try: 
        df = pd.read_csv(CLEAN_DATA_PATH)
        client = get_supabase_client()
        upload_weather(df, client)
        print(f"\nDone. {len(df)} rows uploaded to '{TABLE_NAME}'.")
    except FileNotFoundError:
        print(f"Error: CSV not found at {CLEAN_DATA_PATH}")
    except Exception as e:
        print(f"Upload failed: {e}")
        raise
