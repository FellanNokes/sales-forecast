import pandas as pd


# load sales data
def load_sales_data() -> pd.DataFrame:
    sales = pd.read_csv("data/raw/coffee-shop-sales-revenue.csv", sep="|")
    sales["transaction_date"] = pd.to_datetime(sales["transaction_date"])
    return sales


# load weather data
def load_weather_data() -> pd.DataFrame:
    weather = pd.read_csv("data/cleaned/weather_clean.csv")
    weather["date"] = pd.to_datetime(weather["date"])
    return weather


# merge sales and weather data
def merge_sales_weather(sales: pd.DataFrame, weather: pd.DataFrame) -> pd.DataFrame:
    merged = sales.merge(
        weather,
        left_on="transaction_date",
        right_on="date",
        how="left"
    )
    return merged


# analyze revenue per temperature
def analyze_revenue_per_temperature(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["revenue"] = df["unit_price"] * df["transaction_qty"]

    revenue_by_temperature = (
        df.groupby("temperature_mean")["revenue"]
        .sum()
        .reset_index()
        .sort_values("temperature_mean")
    )

    return revenue_by_temperature


# analyze top selling products
def analyze_top_selling_products(df: pd.DataFrame) -> pd.DataFrame:
    top_products = (
        df.groupby("product_type")["transaction_qty"]
        .sum()
        .reset_index()
        .sort_values("transaction_qty", ascending=False)
    )

    return top_products


# analyze sales vs rain
def analyze_sales_vs_rain(df: pd.DataFrame) -> pd.DataFrame:
    sales_by_rain = (
        df.groupby("rain_sum")["transaction_qty"]
        .sum()
        .reset_index()
        .sort_values("rain_sum")
    )

    return sales_by_rain


if __name__ == "__main__":
    sales = load_sales_data()
    weather = load_weather_data()
    merged = merge_sales_weather(sales, weather)

    print("\nRevenue per temperature:")
    print(analyze_revenue_per_temperature(merged).head())

    print("\nTop selling products:")
    print(analyze_top_selling_products(merged).head())

    print("\nSales vs rain:")
    print(analyze_sales_vs_rain(merged).head())
