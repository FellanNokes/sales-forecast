from pathlib import Path
import pandas as pd
# validate data

base_path = Path(__file__).resolve().parents[2]
RAW_PATH = base_path / "data" / "raw" / "coffee-shop-sales-revenue.csv"
CLEANED_PATH = base_path / "data" / "cleaned" / "coffee-shop-sales-revenue.csv"
REJECTED_PATH = base_path / "data" / "rejected" / "coffee-shop-sales-revenue.csv"


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["transaction_date"] = pd.to_datetime(
        df["transaction_date"].astype("string").str.strip(),
        errors="coerce"
    )

    df["transaction_time"] = pd.to_datetime(
        df["transaction_time"].astype("string").str.strip(),
        format="%H:%M:%S",
        errors="coerce"
    ).dt.time

    df["transaction_qty"] = pd.to_numeric(df["transaction_qty"], errors="coerce")

    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")

    # STRING COLUMNS: strip + title case
    for col in ["store_location", "product_category", "product_type", "product_detail"]:
        df[col] = df[col].astype("string").str.strip().str.title()

    return df


def validate_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Splits the DataFrame into accepted and rejected rows.

    Returns:
        accepted_df: rows that passed all rules
        rejected_df: rows that failed at least one rule, with a 'reason' column
    """
    df = df.copy()

    reject_rules = {
        "missing_transaction_id":   df["transaction_id"].isna(),
        "missing_transaction_date": df["transaction_date"].isna(),
        "missing_transaction_time": df["transaction_time"].isna(),
        "missing_store_id":         df["store_id"].isna(),
        "missing_store_location":   df["store_location"].isna() | (df["store_location"].str.strip() == ""),
        "missing_unit_price":       df["unit_price"].isna(),
        "negative_unit_price":      df["unit_price"].notna() & (df["unit_price"] < 0),
        "missing_qty":              df["transaction_qty"].isna(),
        "non_positive_qty":         df["transaction_qty"].notna() & (df["transaction_qty"] <= 0),
    }

    rules_df = pd.DataFrame(reject_rules)
    reject_mask = rules_df.any(axis=1)

    rejected_df = df[reject_mask].copy()

    if reject_mask.any():
        rejected_df["reason"] = rules_df[reject_mask].apply(
            lambda row: ", ".join(row.index[row.values]), axis=1
        ).values
    else:
        rejected_df["reason"] = pd.NA

    accepted_df = df[~reject_mask].copy()

    return accepted_df, rejected_df


if __name__ == "__main__":
    df = pd.read_csv(RAW_PATH, sep="|")

    cleaned_df = clean_dataframe(df)
    accepted_df, rejected_df = validate_dataframe(cleaned_df)

    accepted_df.to_csv(CLEANED_PATH, index=False)
    rejected_df.to_csv(REJECTED_PATH, index=False)

    print(f"Accepted: {len(accepted_df)} rows -> {CLEANED_PATH}")
    print(f"Rejected: {len(rejected_df)} rows -> {REJECTED_PATH}")

    if not rejected_df.empty:
        print("\nRejected reasons:")
        print(rejected_df["reason"].value_counts())
