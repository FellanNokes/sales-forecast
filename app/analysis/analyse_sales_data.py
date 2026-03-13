import pandas as pd
from app.utility.supabase_functions import fetch_table, upload_dataframe

# ----------------------------#
# SUPABASE - FETCH / UPLOAD
# ----------------------------#


def load_sales_data(table_name: str) -> pd.DataFrame:
    df = fetch_table(table_name)

    df["transaction_date"] = pd.to_datetime(df["transaction_date"])
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["transaction_qty"] = pd.to_numeric(
        df["transaction_qty"], errors="coerce")
    df["product_group"] = df["product_type"].map(PRODUCT_CATEGORY_MAP)

    return df


# Function for uploading to Supabase
def upload_analytics(df: pd.DataFrame, table_name: str) -> None:
    df = df.copy()

    if "transaction_date" in df.columns:
        df["transaction_date"] = df["transaction_date"].astype(str)

    upload_dataframe(df, table_name)

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


# Calculate the total revenue per day based on the store location.
def total_revenue(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["revenue_per_day"] = df["unit_price"] * df["transaction_qty"]

    total_rev = (
        df.groupby(["store_id", "store_location", "transaction_date"])[
            "revenue_per_day"]
        .sum()
        .reset_index()
    )

    return total_rev


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
    df = load_sales_data(SOURCE_TABLE)
    print(f"  {len(df):,} rows loaded.\n")

    print("Adding product_group to historic_sales...")
    upload_analytics(df, SOURCE_TABLE)

    print("Computing and uploading...\n")

    tasks = [
        (total_revenue(df),           "analytics_revenue_by_store"),
        (top5_products(df),           "analytics_top5_products"),
        (top5_products_month(df),     "analytics_top5_products_month"),
        (sales_revenue_per_month(df), "analytics_revenue_per_month"),
        (top_day_revenue_moth(df),    "analytics_top_day_per_month"),
        (least_popular_products(df),  "analytics_least5_popular"),
    ]

    for result_df, table_name in tasks:
        print(f"Uploading → {table_name}")
        upload_analytics(result_df, table_name)

    print("All done!")
