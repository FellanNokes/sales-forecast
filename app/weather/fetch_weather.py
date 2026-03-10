from pathlib import Path
import requests
import pandas as pd

# updatera after talk with felix
# from app.ingest_sales_data.coordinates import get_store_coordinates

LOAD_DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "raw" / "weather_raw.csv"

# DATE RANGE - Load historical data 

START_DATE = "2023-01-01"
END_DATE = "2023-06-30"

DAILY_WEATHER = [
    "weather_code",
    "temperature_2m_mean",
    "temperature_2m_max",
    "temperature_2m_min",
    "rain_sum",
    "snowfall_sum",
    "wind_speed_10m_max",
]

def fetch_all_weather() -> pd.DataFrame:
    coordinates = #get_store_coordinates()
    locations = list(coordinates.keys())
    latitudes = [coordinates[loc][0] for loc in locations]
    longitudes = [coordinates[loc][1] for loc in locations]

    response = requests.get(
        "https://archive-api.open-meteo.com/v1/archive",
        params={
            "latitude": latitudes,
            "longitude": longitudes,
            "start_date": START_DATE,
            "end_date": END_DATE,
            "daily": DAILY_WEATHER,
            "timezone": "America/New_York",
        },
    )
  
    results = response.json()  # list of dicts, one per location

    all_dfs = [] # loops through all 3 locations and creates a df for each one
    
    # to loop through locations
    for location, lat, lon, data in zip(locations, latitudes, longitudes, results):
        print(f"Processing {location} ({lat}, {lon})...")
        df = pd.DataFrame(data["daily"]) # <- turns API response into table with all daily weather params
        df.insert(0, "store_location", location) # store name as 1st column
        df.insert(1, "latitude", lat)
        df.insert(2, "longitude", lon)
        all_dfs.append(df)

    return pd.concat(all_dfs, ignore_index=True) # combines all 3 df.s in one table


if __name__ == "__main__":
    df = fetch_all_weather()
    df.to_csv(LOAD_DATA_PATH, index=False)
    print(f"\nSaved {len(df)} rows to {LOAD_DATA_PATH}")
    print(df.head())
