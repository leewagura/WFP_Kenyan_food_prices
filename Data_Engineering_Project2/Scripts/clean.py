"""
clean.py - Data cleaning / transformation module for WFP Food Prices Kenya.

Handles:
  - Missing value imputation
  - Column name standardization (snake_case)
  - Date parsing and year/month derivation
  - Unit normalisation & price_per_kg calculation
  - Category / commodity name standardization
"""

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Unit → kilograms conversion factors
# Used to derive a normalised `price_per_kg` column.
# ---------------------------------------------------------------------------
UNIT_TO_KG: dict[str, float] = {
    "KG": 1.0,
    "90 KG": 90.0,
    "50 KG": 50.0,
    "13 KG": 13.0,
    "64 KG": 64.0,
    "126 KG": 126.0,
    "400 G": 0.4,
    "200 G": 0.2,
    "L": 1.0,       # 1 litre ≈ 1 kg for liquids (approx)
    "500 ML": 0.5,
    "200 ML": 0.2,
    "Unit": np.nan,  # cannot convert generic "Unit" to kg
    "Bunch": np.nan,
    "Head": np.nan,
}


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns to clean snake_case format."""
    rename_map = {
        "date": "date",
        "admin1": "county",       # admin1 = county-level region in Kenya
        "admin2": "district",     # admin2 = sub-county / district
        "market": "market",
        "market_id": "market_id",
        "latitude": "latitude",
        "longitude": "longitude",
        "category": "category",
        "commodity": "commodity",
        "commodity_id": "commodity_id",
        "unit": "unit",
        "priceflag": "price_flag",
        "pricetype": "price_type",
        "currency": "currency",
        "price": "price",
        "usdprice": "usd_price",
    }
    # Only rename columns that exist
    existing = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(columns=existing)
    logger.info("Standardized %d column names.", len(existing))
    return df


def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Parse the date column and derive year & month."""
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["year"] = df["date"].dt.year.astype("Int64")
    df["month"] = df["date"].dt.month.astype("Int64")
    bad_dates = df["date"].isna().sum()
    if bad_dates:
        logger.warning("%d rows have unparseable dates.", bad_dates)
    return df


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle missing / null values:
      - county, district: fill with 'Unknown'
      - latitude, longitude: forward-fill within same market, else 0
      - price / usd_price: drop rows with missing prices
    """
    for col in ("county", "district"):
        if col in df.columns:
            df[col] = df[col].fillna("Unknown")

    # Forward-fill lat/lon within each market group
    for col in ("latitude", "longitude"):
        if col in df.columns:
            df[col] = df.groupby("market")[col].transform(
                lambda s: s.ffill().bfill()
            )
            df[col] = df[col].fillna(0)

    # Drop rows where price is null (essential field)
    before = len(df)
    df = df.dropna(subset=["price", "usd_price"])
    dropped = before - len(df)
    if dropped:
        logger.info("Dropped %d rows with missing price values.", dropped)

    return df


def standardize_names(df: pd.DataFrame) -> pd.DataFrame:
    """Strip whitespace and title-case categorical text columns."""
    title_cols = ["county", "district", "market", "category", "commodity"]
    for col in title_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.title()
            )

    # Currency should remain uppercase (e.g. KES, USD)
    if "currency" in df.columns:
        df["currency"] = df["currency"].astype(str).str.strip().str.upper()

    return df


def compute_price_per_kg(df: pd.DataFrame) -> pd.DataFrame:
    """
    Derive `price_per_kg` (in local currency) and `usd_price_per_kg`
    using the UNIT_TO_KG mapping.
    """
    df["unit_kg"] = df["unit"].map(UNIT_TO_KG)
    df["price_per_kg"] = np.where(
        df["unit_kg"].notna() & (df["unit_kg"] > 0),
        df["price"] / df["unit_kg"],
        np.nan,
    )
    df["usd_price_per_kg"] = np.where(
        df["unit_kg"].notna() & (df["unit_kg"] > 0),
        df["usd_price"] / df["unit_kg"],
        np.nan,
    )
    # Round to 4 decimal places
    df["price_per_kg"] = df["price_per_kg"].round(4)
    df["usd_price_per_kg"] = df["usd_price_per_kg"].round(4)
    df = df.drop(columns=["unit_kg"])
    logger.info(
        "Computed price_per_kg for %d / %d rows.",
        df["price_per_kg"].notna().sum(),
        len(df),
    )
    return df


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Full cleaning pipeline – called by the orchestrator.

    Steps:
      1. Standardize column names
      2. Parse dates → derive year, month
      3. Handle missing values
      4. Standardize categorical names
      5. Compute price_per_kg
      6. Sort and reset index

    Parameters
    ----------
    df : pd.DataFrame
        Raw extracted dataframe.

    Returns
    -------
    pd.DataFrame
        Cleaned dataframe ready for quality checks & loading.
    """
    logger.info("Starting cleaning pipeline on %d rows.", len(df))

    df = df.copy()
    df = standardize_columns(df)
    df = parse_dates(df)
    df = handle_missing_values(df)
    df = standardize_names(df)
    df = compute_price_per_kg(df)

    # Final sort
    df = df.sort_values(["date", "market", "commodity"]).reset_index(drop=True)

    logger.info("Cleaning complete. Output shape: %s", df.shape)
    return df


# ---------------------------------------------------------------------------
# Standalone test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from extract import extract

    raw = extract(source="local")
    cleaned = clean(raw)
    print(cleaned.head(10))
    print(f"\nShape: {cleaned.shape}")
    print(f"\nNull counts:\n{cleaned.isnull().sum()}")
