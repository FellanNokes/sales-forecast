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
        .reset_index() # <- Groups transaktions per dag and location 
    )

    daily["temperature_mean"] = pd.to_numeric(daily["temperature_mean"], errors="coerce")
    daily["rain_sum"] = pd.to_numeric(daily["rain_sum"], errors="coerce")

    return daily.dropna(subset=["total_revenue", "temperature_mean"])


# STEP 2 — Train model

def train_model(df: pd.DataFrame):
    """
    Train linear regression model
    Features: temperature_mean, rain_sum, weather_condition, temp_category
    Target:   total_revenue
    """
    print("Training model...")

    # Numeric + categorical features
    numeric_features = ["temperature_mean", "rain_sum"]
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
    """Kör prediction och laddar upp till sales_prediction."""

    numeric_features = ["temperature_mean", "rain_sum"]
    categorical_features = ["weather_condition", "temp_category"]

    X_future = forecast_df[numeric_features + categorical_features]
    forecast_df["predicted_revenue"] = model.predict(X_future).clip(min=0)

    # Bygg upp output-df med rätt kolumner
    result = forecast_df[[
        "date",
        "store_location",
        "predicted_revenue",
        "temperature_mean",
        "weather_condition",
        "temp_category",
    ]].rename(columns={"date": "prediction_date"})

    result["model_version"] = "linear_v1"
    result["predicted_revenue"] = result["predicted_revenue"].round(2)
    result["prediction_date"] = result["prediction_date"].astype(str)

    print(f"\nPrediktioner:")
    print(result[["prediction_date", "store_location", "predicted_revenue",
                  "temp_category"]].to_string(index=False))

    upload_dataframe(result, "sales_prediction")
    print(f"\nKlart. {len(result)} prediktioner uppladdade till 'sales_prediction'.")