import pandas as pd
import ingest_data as data
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

df1 = data.get_df()

geolocator = Nominatim(user_agent="geo_project")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

def get_coordinates(location):
    loc = geocode(location)
    if loc:
        return pd.Series([loc.latitude, loc.longitude])
    return pd.Series([None, None])

# skapa location
df1["full_location"] = df1["store_location"] + ", United States"

# ta bara unika locations
unique_locations = df1["full_location"].drop_duplicates()

# geokoda dem
coords = unique_locations.apply(get_coordinates)

# gör dataframe
coords_df = coords
coords_df.index = unique_locations
coords_df.columns = ["latitude", "longitude"]

# merge tillbaka
df1 = df1.join(coords_df, on="full_location")

print(df1[["latitude", "longitude", "store_location"]].head(43))