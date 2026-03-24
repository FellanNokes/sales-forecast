import pandas as pd
from app.utility.supabase_functions import fetch_table, upload_dataframe

# fetch 

def fetch_data() -> pd.DataFrame:
    print("Fetching data from Supabase...")
    df = fetch_table("sales_weather_joined")
    print(f"Fetched {len(df)} rows.")
    return df


# aggregate 

def aggregate_daily(df: pd.DataFrame) -> pd.DataFrame:

    
    daily = (
        df.groupby([
            "transaction_date",
            "store_location",
            "temperature_mean",
            "rain_sum",
            "temp_category",
            "weather_condition",
        ])
        .agg(
            total_revenue=("unit_price", "sum"),
            total_qty=("transaction_qty", "sum"),
        )
        .reset_index()
    )
 
    daily["avg_revenue"] = daily["total_revenue"] / daily["total_qty"]
 
    return daily


# correlation

def run_correlations(daily: pd.DataFrame) -> pd.DataFrame:

    results = []
    
    # Numerisk korrelation
    results.append({
        "analysis_date": pd.Timestamp.today().date().isoformat(),
        "metric": "temperature_vs_revenue",
        "correlation_value": round(daily["temperature_mean"].corr(daily["total_revenue"]), 4)
    })
    results.append({
        "analysis_date": pd.Timestamp.today().date().isoformat(),
        "metric": "rain_vs_revenue",
        "correlation_value": round(daily["rain_sum"].corr(daily["total_revenue"]), 4)
    })

    # Per vädertyp
    weather_corr = daily.groupby("weather_condition")["total_revenue"].mean().round(2)
    for condition, avg_revenue in weather_corr.items():
        results.append({
            "analysis_date": pd.Timestamp.today().date().isoformat(),
            "metric": f"avg_revenue_{condition}",
            "correlation_value": avg_revenue
        })

    # Per temperaturkategori
    temp_corr = daily.groupby("temp_category")["total_revenue"].mean().round(2)
    for category, avg_revenue in temp_corr.items():
        results.append({
            "analysis_date": pd.Timestamp.today().date().isoformat(),
            "metric": f"avg_revenue_{category}",
            "correlation_value": avg_revenue
        })

    df_results = pd.DataFrame(results)
    print(df_results)
    return df_results

# MAIN

if __name__ == "__main__":
    df = fetch_data()
    daily = aggregate_daily(df)
    
    df_correlations = run_correlations(daily)
    
    upload_dataframe(daily, "weather_sales_summary")
    upload_dataframe(df_correlations, "weather_correlation_results")
    
    print(f"\nDone. {len(daily)} rows uploaded to 'weather_sales_summary'.")
    print(f"Done. {len(df_correlations)} rows uploaded to 'weather_correlation_results'.")