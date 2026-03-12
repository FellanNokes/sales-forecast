import pandas as pd
from pathlib import Path

CLEAN_DATA_PATH = Path(__file__).resolve().parents[2] / "data" / "cleaned" / "weather_clean.csv"
FEATURES_DATA_PATH = Path(__file__).resolve().parents[2] / "data" / "cleaned" / "weather_features.csv"



# ----------------------------------------
#         TEMPERATURE LABELS  
# ----------------------------------------

def categorize_temperature(temp: float) -> str:
    if temp < 0:
        return "freezing"
    elif temp < 9:
        return "cold"
    elif temp < 17:
        return "mild"
    else:
        return "warm"
    
