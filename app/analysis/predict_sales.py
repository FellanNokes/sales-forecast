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
    print("Connecting to Supabase...")
    df = fetch_table("sales_weather_joined")
    print(f"Fetched {len(df)} rows.")


    print("Calculating revenue...")
    df["revenue"] = pd.to_numeric(df["unit_price"], errors="coerce") * \
                    pd.to_numeric(df["transaction_qty"], errors="coerce")


    print("Aggregating daily revenue...") 
    # Group transaktions per dag and location 
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
    print(f"Aggregated to {len(daily)} daily rows.")

    print("Converting to numeric...") 
    daily["temperature_mean"] = pd.to_numeric(daily["temperature_mean"], errors="coerce")
    daily["rain_sum"] = pd.to_numeric(daily["rain_sum"], errors="coerce")

    # Add day of week and month as features
    daily["transaction_date"] = pd.to_datetime(daily["transaction_date"])
    daily["day_of_week"] = daily["transaction_date"].dt.dayofweek  # 0=Monday, 6=Sunday
    daily["month"] = daily["transaction_date"].dt.month

    print("Dropping nulls...") 
    result = daily.dropna(subset=["total_revenue", "temperature_mean"])

    print(f"Final training rows: {len(result)}")
    return result

# STEP 2 — Train model

def train_model(df: pd.DataFrame):
    """
    Train linear regression model
    Features: temperature_mean, rain_sum, weather_condition, temp_category
    Target:   total_revenue
    """
    print("Training model...")

    # Numeric + categorical features
    numeric_features = ["temperature_mean", "rain_sum", "day_of_week", "month"]
    categorical_features = ["weather_condition", "temp_category"]

    # OneHotEncoder = transforms categorical feat to binary columns so the model can use them
    preprocessor = ColumnTransformer(transformers=[
        ("num", "passthrough", numeric_features), # <- numeric feat don't need to go through the encoder
        ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_features), # categorical feat goes through encoder
    ])  # handle_unknown -> handels unknown feat


    """
    Pipeline connects the preprocessing and modelling in one step.
    1. Preprocessor start (OneHotEncoder etc)
    2. Train LinearRegression on the processed data.
    """
    model = Pipeline(steps=[
        ("preprocessor", preprocessor),
        ("regressor", LinearRegression()),
    ])

    X = df[numeric_features + categorical_features]
    # X = all features (what the model sees)
    y = df["total_revenue"]
    # y = what the model is learning to predict

    
    # model.fit() trains the model on historical data.
    # It reads every (weather, revenue) pair and adjusts
    # internal coefficients until predictions match reality.
    # After this step the model is ready to predict future revenue  
    model.fit(X, y) 

    # Scoring the model 
    score = model.score(X, y)
    print(f"  R² score: {score:.3f} (explains {score*100:.1f}% of variation)")

    return model


# STEP 3 — Get weather forecast

def load_forecast() -> pd.DataFrame:
    """Get's 14-day weather forecast from Supabase and adds features."""
    print("Getting weather from weather_forecast...")
    df = fetch_table("weather_forecast")

    # Add same categories as training data
    from app.weather.weather_features import categorize_temperature, WEATHER_CODE_MAP
    df["temperature_mean"] = pd.to_numeric(df["temperature_mean"], errors="coerce")
    df["rain_sum"] = pd.to_numeric(df["rain_sum"], errors="coerce").fillna(0)
    df["temp_category"] = df["temperature_mean"].apply(categorize_temperature)
    df["weather_condition"] = df["weather_code"].map(WEATHER_CODE_MAP).fillna("cloudy")

    # Normalisera store_location (Remowe ", New York, United States" suffix to match column name in sales_weather_joined)
    df["store_location"] = (
        df["store_location"]
        .str.replace(r",.*$", "", regex=True)
        .str.strip()
    )

    return df


# STEP 4 — Create prediction and save

def predict_and_upload(model, forecast_df: pd.DataFrame) -> None:
    """Run prediction and upload to sales_prediction table in Supabase."""

    forecast_df["transaction_date"] = pd.to_datetime(forecast_df["date"])
    forecast_df["day_of_week"] = forecast_df["transaction_date"].dt.dayofweek
    forecast_df["month"] = forecast_df["transaction_date"].dt.month

    numeric_features = ["temperature_mean", "rain_sum", "day_of_week", "month"]
    categorical_features = ["weather_condition", "temp_category"]

    X_future = forecast_df[numeric_features + categorical_features]
    # Collects the weather features used for the model
    forecast_df["predicted_revenue"] = model.predict(X_future).clip(min=0)
    # model.predict(X_future) is wher ethe model is actually used. 
    # clip(min=0) ensures predictions are never negative.
    # A linear model can mathematically produce negative values but negative revenue is not possible

    # Choose columns to save to Supabase and update column name to match
    result = forecast_df[[
        "date",
        "store_location",
        "predicted_revenue",
        "temperature_mean",
        "weather_condition",
        "temp_category",
        "day_of_week",
    ]].rename(columns={"date": "prediction_date"})

    result["model_version"] = "linear_v1" # <- save which version of the model is being used
    result["predicted_revenue"] = result["predicted_revenue"].round()
    result["prediction_date"] = result["prediction_date"].astype(str)

    print(f"\nPrediction:")
    print(result[["prediction_date", "store_location", "predicted_revenue",
                  "temp_category"]].to_string(index=False))
    
    # Delete existing predictions before uploading new ones
    from app.utility.supabase_functions import _get_client

    client = _get_client()
    client.table("sales_prediction").delete().neq("id", 0).execute()

    upload_dataframe(result, "sales_prediction")

    print(f"\nDone. {len(result)} predictions uploaded to 'sales_prediction'.")


if __name__ == "__main__":
    print("Script started")
    training_data = load_training_data()
    print(f"Training data: {len(training_data)} daily rows\n")
    model = train_model(training_data)
    forecast = load_forecast()
    predict_and_upload(model, forecast)