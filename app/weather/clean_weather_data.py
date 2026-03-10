import pandas as pd

# import the fetch weather function
from fetch_weather import fetch_all_weather

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
    df.columns.str.replace(" ", "").str.replace("_", "-")

    # Date
    df["time"] = pd.to_datetime(df["time"], errors="coerce", utc=True)

    # Numeric columns
    for col in NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # WeatherCode - check that it is an integer and convert it
    df["weather_code"] = df["weather_code"].astype("Int64")

    # return the transformed DataFrame
    return df


# -----------------------#
####### REEJCT    #######
# -----------------------#

# Function that rejects invalid data

def reject_invalid_data(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:

    # Definition of the rejected data values
    rejected_data = (
        df["time"].isna() |
        df["temperature_2m_mean"].isna() |
        df["rain_sum"].isna() |
        df["snowfall_sum"].isna()
    )

    # copy and seperate the rejected and valid data based on T/F
    df_rejected = df[rejected_data].copy()
    df_valid = df[~rejected_data].copy()

    return df_valid, df_rejected


# -----------------------#
####### MAIN    #######
# -----------------------#

if __name__ == "__main__":
    df = fetch_all_weather()
    transformed_df = transform_data(df)
    clean_df, rejected_df = reject_invalid_data(transformed_df)
    print(clean_df.head())
