import pandas as pd

# load datasets
sales = pd.read_csv("data/raw/coffee-shop-sales-revenue.csv", sep="|")
weather = pd.read_csv("data/raw/weather.csv")

# merge sales with weather by date and location
merged = sales.merge(
    weather,
    left_on=["transaction_date", "store_location"],
    right_on=["date", "store_location"],
    how="left"
)


#  revenue per temperature


temp_sales = merged.groupby("temperature_mean")["unit_price"].sum()

print("\nRevenue by temperature:")
print(temp_sales.head())


# top selling products


top_products = merged.groupby("product_type")[
    "transaction_qty"].sum().sort_values(ascending=False)

print("\nTop selling products:")
print(top_products.head())


# rain vs sales


rain_sales = merged.groupby("rain_sum")["transaction_qty"].sum()

print("\nSales vs rain:")
print(rain_sales.head())