"""
Trains a linear regression model on historical data from 
sales_weather_joined and predicts revenue for the next 14 days 
based on weather_forecast. 

Run: uv run -m app.analysis.predict_sales

"""
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
import numpy as np
from app.utility.supabase_functions import fetch_table, upload_dataframe


# STEP 1 — Get och prepare training data

def load_training_data() -> pd.DataFrame:
    """Get historic data and aggregate to dailt revenue per store."""
    print("Getting training datat from sales_weather_joined...")
    df = fetch_table("sales_weather_joined")

    df["revenue"] = pd.to_numeric(df["unit_price"], errors="coerce") * \
                    pd.to_numeric(df["transaction_qty"], errors="coerce")

    daily = (
        df.groupby([
            "transaction_date",
            "store_location",
            "temperature_mean",
            "rain_sum",
            "temp_category",
            "weather_condition",
        ])
        .agg(total_revenue=("revenue", "sum"))
        .reset_index()
    )

    daily["temperature_mean"] = pd.to_numeric(daily["temperature_mean"], errors="coerce")
    daily["rain_sum"] = pd.to_numeric(daily["rain_sum"], errors="coerce")

    return daily.dropna(subset=["total_revenue", "temperature_mean"])