"""
clean1.py — Data cleaning for wfp_food_prices_ken.csv

Steps:
  1. Load the raw CSV
  2. Enforce correct data types (date, numeric, string columns)
  3. Drop rows with any null values
  4. Remove duplicate rows
  5. Save the cleaned CSV
"""

import pandas as pd

INPUT_CSV = "wfp_food_prices_ken.csv"
OUTPUT_CSV = "wfp_food_prices_ken_cleaned.csv"


def clean_data(input_path: str = INPUT_CSV, output_path: str = OUTPUT_CSV) -> pd.DataFrame:
    df = pd.read_csv(input_path)
    rows_before = len(df)
    print(f"Loaded {rows_before} rows from {input_path}")

    # ── 1. Fix data types ──────────────────────────────────────

    # date column: coerce invalid dates to NaT so they get dropped with nulls
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Integer IDs (nullable Int64 to tolerate NaN before drop)
    df["market_id"] = pd.to_numeric(df["market_id"], errors="coerce").astype("Int64")
    df["commodity_id"] = pd.to_numeric(df["commodity_id"], errors="coerce").astype("Int64")

    # Numeric columns
    for col in ("latitude", "longitude", "price", "usdprice"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # String columns — strip whitespace
    str_cols = ["admin1", "admin2", "market", "category", "commodity",
                "unit", "priceflag", "pricetype", "currency"]
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()
        # pandas reads NaN as the string "nan" after astype(str)
        df[col] = df[col].replace("nan", pd.NA)

    print("\nData types after casting:")
    print(df.dtypes)

    # ── 2. Drop null rows ──────────────────────────────────────

    nulls_before = df.isnull().sum()
    total_nulls = nulls_before.sum()
    if total_nulls:
        print(f"\nNull counts per column:\n{nulls_before[nulls_before > 0]}")
    else:
        print("\nNo null values found.")

    df = df.dropna()
    rows_after_nulls = len(df)
    print(f"\nDropped {rows_before - rows_after_nulls} rows with nulls "
          f"({rows_after_nulls} remaining)")

    # ── 3. Remove duplicates ───────────────────────────────────

    dupes = df.duplicated().sum()
    print(f"\nDuplicate rows found: {dupes}")
    if dupes:
        df = df.drop_duplicates()
        print(f"After removing duplicates: {len(df)} rows remaining")

    # ── 4. Reset index and save ────────────────────────────────

    df = df.reset_index(drop=True)
    df.to_csv(output_path, index=False)
    print(f"\nCleaned data saved to {output_path} ({len(df)} rows)")

    return df


if __name__ == "__main__":
    clean_data()
