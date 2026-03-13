
from supabase import create_client, Client
import pandas as pd
from dotenv import load_dotenv
import os

# Setup the Supabase connection
load_dotenv()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# Fetch all cleaned sales data from supabase


def fetch_all_sales_data(supabase: Client, table_name: str) -> pd.DataFrame:

    fetched_data = []
    # Batch size to fetch (max limit on supabase is 1000)
    batch_size = 1000
    start = 0

    # While there is data left to fetch - do it
    while True:
        response = supabase.table(table_name).select(
            "*").range(start, start + batch_size - 1).execute()

        # if there is no data left to fetch - break
        if not response.data:
            break

        # add the data to list
        fetched_data.extend(response.data)

        # increase start by batch_size
        start += batch_size

    # Create dataframe with fetched data
    df = pd.DataFrame(fetched_data)

    return df


# Calculate the total revenue per day based on the store location
def total_revenue(df: pd.DataFrame):
    df["revenue_per_day"] = df["unit_price"] * df["transaction_qty"]

    total_revenue_store = (
        df.groupby(["store_id", "store_location", "transaction_date"])[
            "revenue_per_day"]
        .sum()
        .reset_index()
    )

    # Create a dictonary with DF per store
    store_dfs = {
        store: total_revenue_store[total_revenue_store["store_location"] == store].reset_index(
            drop=True)
        for store in total_revenue_store["store_location"].unique()
    }

    # Print each store
    for store, store_df in store_dfs.items():
        print(f"\n--- {store} ---")
        print(store_df.head())

    return store_dfs


# Top 5 products total period
def top5_products(df: pd.DataFrame):

    # Top 5 products every month


def top5_products_month(df: pd.DataFrame) -> pd.DataFrame:

    # Sales revenue per month


def sales_revenue_per_month(df: pd.DataFrame):

    # Highest daily rev per month


def top_day_revenue_moth(df: pd.DataFrame):


def total_revenue_per_day(df: pd.DataFrame):

    # Add extra column for additional product category(Drinks, Food, ...)

    # Least popular products total
