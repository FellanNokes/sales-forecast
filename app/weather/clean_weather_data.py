import pandas as pd
from pathlib import Path

# import the fetch weather function
from app.weather.fetch_weather import fetch_all_weather

CLEAN_DATA_PATH = Path(__file__).resolve().parents[2] / "data" / "cleaned" / "weather_clean.csv"
REJECTED_DATA_PATH = Path(__file__).resolve().parents[2] / "data" / "rejected" / "weather_rejected.csv"

# -----------------------#
####### TRANSFORM #######
# -----------------------#

# numeric columns
NUMERIC_COLS = [
    "weather_code",
    "temperature_2m_mean",
    "temperature_2m_max",
    "temperature_2m_min",
    "rain_sum",
    "snowfall_sum",
    "wind_speed_10m_max",
]

# Function to transform data from weather API


def transform_data(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    df.columns = df.columns.str.strip()
   
    # do we need this?
    #df.columns.str.replace(" ", "").str.replace("_", "-")
    df.columns =df.columns.str.replace(" ", "")

    # Date
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    #df["time"] = pd.to_datetime(df["time"], errors="coerce", utc=True)

    # Numeric columns
    for col in NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # WeatherCode - check that it is an integer and convert it
    df["weather_code"] = df["weather_code"].astype("Int64")

    # change column names
    df = df.rename(columns={
        "time": "date",
        "temperature_2m_mean": "temperature_mean",
        "temperature_2m_max": "temperature_max",
        "temperature_2m_min": "temperature_min",
        "wind_speed_10m_max": "wind_speed_max",
    })

    # return the transformed DataFrame
    return df


# -----------------------#
####### REEJCT    #######
# -----------------------#

# Function that rejects invalid data

def reject_invalid_data(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:

    # Definition of the rejected data values
    rejected_data = (
        df["date"].isna() |
        df["temperature_mean"].isna() |
        df["rain_sum"].isna() |
        df["snowfall_sum"].isna() |
        (df["rain_sum"] < 0) |
        (df["snowfall_sum"] < 0) |
        # check that tthe max temprature cant be lower than the min temperature
        (df["temperature_max"] < df["temperature_min"]) |
        (df["temperature_max"] > 60) |
        (df["temperature_min"] < -70)
    )

    # copy and seperate the rejected and valid data based on T/F
    df_rejected = df[rejected_data].copy()
    df_valid = df[~rejected_data].copy()

    # Identify duplicates in the data
    duplicates = df_valid.duplicated(
        subset=["store_location", "date"], keep="first")

    # Add duplicates to df_refejted
    df_rejected = pd.concat([df_rejected, df_valid[duplicates]])

    # remove duplicated from valid data
    df_valid = df_valid[~duplicates]

    return df_valid, df_rejected


# -----------------------#
####### MAIN    #######
# -----------------------#

if __name__ == "__main__":
    df = fetch_all_weather()
    transformed_df = transform_data(df)
    clean_df, rejected_df = reject_invalid_data(transformed_df)
    clean_df.to_csv(CLEAN_DATA_PATH, index=False)
    rejected_df.to_csv(REJECTED_DATA_PATH, index=False)
    print(f"\nSaved {len(df)} rows to {CLEAN_DATA_PATH}")
    print(clean_df.head(50))
