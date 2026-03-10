from typing import Union

import pandas as pd
from pathlib import Path
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# --- Setup ---
geolocator = Nominatim(user_agent="geo_project")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)


def get_coordinates(location: str) -> Union[tuple[float, float], tuple[None, None]]:
    """
    Returns (latitude, longitude) for a location string, or (None, None) if not found.

    Args:
        location: A human-readable location string, e.g. "Lower Manhattan, New York, United States"

    Returns:
        Tuple of (latitude, longitude) as floats, or (None, None) if geocoding fails.

    Example:
        >>> lat, lon = get_coordinates("Lower Manhattan, New York, United States")
        >>> print(lat, lon)
        40.7127 -74.0059
    """
    loc = geocode(location)
    if loc:
        return loc.latitude, loc.longitude
    return None, None


def get_coordinates_df(locations: list[str]) -> pd.DataFrame:
    """
    Geocodes a list of location strings and returns a DataFrame with lat/lon.

    Deduplicates locations before geocoding to minimize API calls.
    Results can be joined back to your main DataFrame on the location column.

    Args:
        locations: List of location strings (duplicates are handled automatically).

    Returns:
        DataFrame indexed by location string with columns ['latitude', 'longitude'].

    Example:
        >>> locations = ["Lower Manhattan, New York, United States", "Hell's Kitchen, New York, United States"]
        >>> coords_df = get_coordinates_df(locations)
        >>> print(coords_df)
    """
    unique_locations = pd.Series(locations).drop_duplicates()

    coords = unique_locations.apply(lambda loc: pd.Series(get_coordinates(loc), index=["latitude", "longitude"]))
    coords.index = unique_locations.values

    return coords


# --- Load & enrich the sales data ---
def load_sales_with_coordinates() -> pd.DataFrame:
    """
    Loads the coffee shop sales CSV, geocodes store locations,
    and returns the full DataFrame with latitude and longitude columns.
    """
    ##TODO: This should come from ingest_data
    base_path = Path(__file__).resolve().parents[2]
    file_path = base_path / "data" / "raw" / "coffee-shop-sales-revenue.csv"

    df = pd.read_csv(file_path, sep="|")
    df["full_location"] = df["store_location"] + ", United States"

    coords_df = get_coordinates_df(df["full_location"].tolist())
    df = df.join(coords_df, on="full_location")

    return df


"""if __name__ == "__main__":
    # Quick test — geocode a couple of locations manually
    test_locations = [
        "Lower Manhattan, New York, United States",
        "Hell's Kitchen, New York, United States",
        "Astoria, New York, United States",
    ]

    print("Testing get_coordinates_df():")
    coords = get_coordinates_df(test_locations)
    print(coords)
    print()

    print("Testing load_sales_with_coordinates():")
    df = load_sales_with_coordinates()
    print(df[["store_location", "latitude", "longitude", "transaction_date"]].head(10))"""