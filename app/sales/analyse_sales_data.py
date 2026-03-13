
from supabase import create_client, Client
import pandas as pd
from dotenv import load_dotenv
import os

# ----------------------------#
# SUPABASE - FETCH / UPLOAD
# ----------------------------#

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

    # Ensure correct dtypes
    df["transaction_date"] = pd.to_datetime(df["transaction_date"])
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["transaction_qty"] = pd.to_numeric(
        df["transaction_qty"], errors="coerce")

    # Add product_grop
    df["product_group"] = df["product_type"].map(PRODUCT_CATEGORY_MAP)

    df["transaction_date"] = df["transaction_date"].astype(str)

    return df


# Function for uploading to Supabase
def upload_sales_analytics(supabase: Client, df: pd.DataFrame, table_name: str, batch_size: int = 500):

    df = df.copy()

    records = df.to_dict(orient="records")
    total = len(records)
    uploaded = 0

    for i in range(0, total, batch_size):
        batch = records[i: i + batch_size]
        try:
            supabase.table(table_name).upsert(batch).execute()
            uploaded += len(batch)
            print(f"  [{table_name}] {uploaded}/{total} rows uploaded")
        except Exception as e:
            print(f"  [{table_name}] ERROR on batch {i}-{i + batch_size}: {e}")

    print(f"  [{table_name}] done\n")

# ----------------------------#
# SALES ANALYTICS
# ----------------------------#


# Map for the different categories
PRODUCT_CATEGORY_MAP = {
    # Drinks
    "Brewed Chai Tea": "Drinks",
    "Brewed Green Tea": "Drinks",
    "Brewed Black Tea": "Drinks",
    "Brewed Herbal Tea": "Drinks",
    "Premium Brewed Coffee": "Drinks",
    "Organic Brewed Coffee": "Drinks",
    "Gourmet Brewed Coffee": "Drinks",
    "Barista Espresso": "Drinks",
    "Drip Coffee": "Drinks",
    "Organic Chocolate": "Drinks",
    "Drinking Chocolate": "Drinks",
    "Hot Chocolate": "Drinks",
    "Regular Syrup": "Drinks",
    "Sugar Free Syrup": "Drinks",
    # Bakery
    "Biscotti": "Bakery",
    "Pastry": "Bakery",
    "Scone": "Bakery",
    # Branded
    "Clothing": "Branded",
    "Housewares": "Branded",
    # Packaged
    "Espresso Beans": "Packaged",
    "Gourmet Beans": "Packaged",
    "House Blend Beans": "Packaged",
    "Organic Beans": "Packaged",
    "Premium Beans": "Packaged",
    "Green Beans": "Packaged",
    "Black Tea": "Packaged",
    "Herbal Tea": "Packaged",
    "Green Tea": "Packaged",
    "Chai Tea": "Packaged",
}


# # Add product category column
# def add_prod_cat_col(df: pd.DataFrame):

#     df = df.copy()
#     df["product_group"] = df["product_type"].map(PRODUCT_CATEGORY_MAP)

#     return df


# Calculate the total revenue per day based on the store location.
def total_revenue(df: pd.DataFrame):
    df = df.copy()
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

    return store_dfs


# Top 5 products total period
def top5_products(df: pd.DataFrame):
    df = df.copy()

    df["revenue"] = df["unit_price"] * df["transaction_qty"]

    top5 = (
        df.groupby(["store_location", "product_type"])["revenue"]
        .sum()
        .reset_index()
        .sort_values(["store_location", "revenue"], ascending=[True, False])
        .groupby("store_location", group_keys=False)
        .head(5)
        .reset_index(drop=True)

    )

    return top5


# Top 5 products every month
def top5_products_month(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    # Convert the transaction date to months
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
    df["month_num"] = df['transaction_date'].dt.month
    # Fix the month to text (jan, feb)
    df['month'] = df['transaction_date'].dt.strftime('%B')

    # Revenue per day
    df["revenue_per_day"] = df["unit_price"] * df["transaction_qty"]

    # Group by to achieve the rev_per_day and sort by the 5 largest each month per store
    top5_prod_store = (
        df.groupby(['store_id', 'store_location', 'month',
                   'month_num', 'product_type'])['revenue_per_day']
        .sum()
        .reset_index()
        .groupby(['store_id', 'month_num'], group_keys=False)
        .apply(lambda x: x.nlargest(5, 'revenue_per_day'))
        .sort_values(['store_id', 'month_num'])
        .drop(columns=['month_num'])
        .reset_index(drop=True)
    )

    return top5_prod_store


# Sales revenue per month
def sales_revenue_per_month(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["month_num"] = df["transaction_date"].dt.month
    df["month"] = df["transaction_date"].dt.strftime("%B")
    df["revenue"] = df["unit_price"] * df["transaction_qty"]

    sales_monthly = (
        df.groupby(["month_num", "month", "store_location"])["revenue"]
        .sum()
        .reset_index()
        .sort_values("month_num")
        .drop(columns=["month_num"])
        .reset_index(drop=True)
    )

    return sales_monthly


# Highest daily rev per month
def top_day_revenue_moth(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["month_num"] = df["transaction_date"].dt.month
    df["month"] = df["transaction_date"].dt.strftime("%B")
    df["revenue"] = df["unit_price"] * df["transaction_qty"]

    sales = (
        df.groupby(["month_num", "month", "store_location",
                   "transaction_date"])["revenue"]
        .sum()
        .reset_index()
    )

    daily_sales = (
        sales.sort_values("revenue", ascending=False)
        .groupby(["month_num", "store_location"], as_index=False)
        .first()
        .sort_values(["store_location", "month_num"])
        .drop(columns=["month_num"])
        .reset_index(drop=True)
    )

    return daily_sales


# Least popular products total in each store location
def least_popular_products(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["month_num"] = df["transaction_date"].dt.month
    df["month"] = df["transaction_date"].dt.strftime("%B")
    df["least_revenue"] = df["unit_price"] * df["transaction_qty"]

    least_prod_sales = (
        df.groupby(["store_location", "product_type"])["least_revenue"]
        .sum()
        .reset_index()
        .sort_values(["store_location", "least_revenue"], ascending=True)
        .groupby("store_location", group_keys=False)
        .head(5)
        .reset_index(drop=True)
    )

    return least_prod_sales


# ----------------------------#
# MAIN
# ----------------------------#
if __name__ == "__main__":
    SOURCE_TABLE = "historic_sales"

    print("Fetching data...")
    df = fetch_all_sales_data(supabase, SOURCE_TABLE)
    print(f"  {len(df):,} rows loaded.\n")

    print("Computing and uploading...\n")

    # total_revenue returnerar en dict — flatten till en DataFrame
    store_flat = pd.concat(total_revenue(df).values(), ignore_index=True)

    upload_sales_analytics(supabase, store_flat,
                           "analytics_revenue_by_store")
    upload_sales_analytics(supabase, top5_products(
        df),           "analytics_top5_products")
    upload_sales_analytics(supabase, top5_products_month(
        df),     "analytics_top5_products_month")
    upload_sales_analytics(supabase, sales_revenue_per_month(
        df), "analytics_revenue_per_month")
    upload_sales_analytics(supabase, top_day_revenue_moth(
        df),    "analytics_top_day_per_month")
    upload_sales_analytics(supabase, least_popular_products(
        df),  "analytics_least5_popular")

    print("All done!")
