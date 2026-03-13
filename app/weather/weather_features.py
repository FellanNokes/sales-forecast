import pandas as pd
from pathlib import Path

CLEAN_DATA_PATH = Path(__file__).resolve().parents[2] / "data" / "cleaned" / "weather_clean.csv"
FEATURES_DATA_PATH = Path(__file__).resolve().parents[2] / "data" / "cleaned" / "weather_features.csv"




#  TEMPERATURE LABELS  
# --------------------

def categorize_temperature(temp: float) -> str:
    if temp < 0:
        return "freezing"
    elif temp < 9:
        return "cold"
    elif temp < 17:
        return "mild"
    else:
        return "warm"
    
    

# WEATHER CODE LABELS 
# ---------------------
 
WEATHER_CODE_MAP = {
    0:  "clear",
    1:  "clear",
    2:  "cloudy",
    3:  "cloudy",
    45: "foggy",
    48: "foggy",
    51: "drizzle",
    53: "drizzle",
    55: "drizzle",
    56: "freezing_drizzle",
    57: "freezing_drizzle",
    61: "rain",
    63: "rain",
    65: "rain",
    66: "freezing_rain",
    67: "freezing_rain",
    71: "snow",
    73: "snow",
    75: "snow",
    77: "heavy_snow",
    80: "rain_showers",
    81: "rain_showers",
    82: "rain_showers",
    85: "snow_showers",
    86: "snow_showers",
    95: "thunderstorm",
    96: "thunderstorm",
    99: "thunderstorm",
}



# CREATE WEATHER FEATURE TABLE
# -----------------------------

def add_weather_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Temperature category
    df["temp_category"] = df["temperature_mean"].apply(categorize_temperature)

    # Weather condition
    df["weather_condition"] = df["weather_code"].map(WEATHER_CODE_MAP).fillna("unknown")


    # Keep columns needed for features table
    df_features = df[[
        "store_location",
        "date",
        "weather_code",
        "weather_condition",
        "temperature_mean",
        "temp_category",
        "rain_sum",
        "snowfall_sum",
    ]]
 
    return df_features  


# MAIN           
# -----
 
if __name__ == "__main__":
    df = pd.read_csv(CLEAN_DATA_PATH)
    df_features = add_weather_features(df)
    df_features.to_csv(FEATURES_DATA_PATH, index=False)
    print(f"Saved {len(df_features)} rows to {FEATURES_DATA_PATH}")
    print(df_features.head(10))
    print("\nValue counts — temp_category:")
    print(df_features["temp_category"].value_counts())
    print("\nValue counts — weather_condition:")
    print(df_features["weather_condition"].value_counts())