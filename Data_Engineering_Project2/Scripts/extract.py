"""
extract.py - Data extraction module for WFP Food Prices Kenya pipeline.

Downloads CSV from a URL (WFP data API / HDX) or loads a local CSV file.
Supports incremental extraction by filtering on dates already processed.
"""

import os
import logging
import pandas as pd
import requests

logger = logging.getLogger(__name__)

# Default remote URL for WFP Kenya food prices dataset (HDX mirror)
DEFAULT_URL = (
    "https://data.humdata.org/dataset/"
    "e0d3fba6-f9a2-45d7-b949-140c455197ff/resource/"
    "517ee1bf-2437-4f8c-aa1b-cb9925b9d437/download/"
    "wfp_food_prices_ken.csv"
)

# Local fallback path (relative to project root)
LOCAL_CSV = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "wfp_food_prices_ken.csv",
)


def extract_from_url(url: str = DEFAULT_URL, timeout: int = 120) -> pd.DataFrame:
    """
    Download CSV from a remote URL and return as a DataFrame.

    Parameters
    
    url : str
        URL pointing to the CSV resource.
    timeout : int
        HTTP request timeout in seconds.

    Returns
    
    pd.DataFrame
        Raw dataframe read from the remote CSV.
    """
    logger.info("Downloading data from URL: %s", url)
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()

        # Write to a temp file, then read with pandas
        tmp_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "_tmp_download.csv"
        )
        with open(tmp_path, "wb") as f:
            f.write(response.content)

        df = pd.read_csv(tmp_path)
        os.remove(tmp_path)
        logger.info("Downloaded %d rows from remote source.", len(df))
        return df

    except requests.RequestException as exc:
        logger.warning("Remote download failed (%s). Falling back to local file.", exc)
        return extract_from_local()


def extract_from_local(filepath: str = LOCAL_CSV) -> pd.DataFrame:
    """
    Load the CSV from a local file path.

    Parameters
    
    filepath : str        Absolute or relative path to the CSV file.

    Returns
    
    pd.DataFrame
        Raw dataframe read from the local CSV.
    """
    logger.info("Loading local CSV: %s", filepath)
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Local CSV not found: {filepath}")

    df = pd.read_csv(filepath)
    logger.info("Loaded %d rows from local file.", len(df))
    return df


def extract(
    source: str = "local",
    url: str = DEFAULT_URL,
    filepath: str = LOCAL_CSV,
    last_processed_date: str | None = None,
) -> pd.DataFrame:
    """
    Main extraction entry-point.

    Parameters
    
    source : str
        'url' to download from the web, 'local' to load from disk.
    url : str
        Remote URL (if source='url').
    filepath : str
        Local file path (if source='local').
    last_processed_date : str or None
        only rows with: date > last_processed_date are returned (incremental logic).

    Returns
    
    pd.DataFrame
        Extracted dataframe.
    """
    if source == "url":
        df = extract_from_url(url)
    else:
        df = extract_from_local(filepath)

    # Incremental logic: keep only new rows 
    if last_processed_date is not None:
        original_len = len(df)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df[df["date"] > pd.Timestamp(last_processed_date)].copy()
        logger.info(
            "Incremental filter: kept %d / %d rows (after %s).",
            len(df),
            original_len,
            last_processed_date,
        )

    return df


# Quick standalone test

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    data = extract(source="local")
    print(data.head())
    print(f"\nShape: {data.shape}")
