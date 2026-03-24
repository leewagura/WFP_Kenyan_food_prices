"""
load.py - Load module for WFP Food Prices Kenya pipeline.

Loads cleaned data into PostgreSQL:
  1. Staging table (flat cleaned data)
  2. Dimension tables (dim_date, dim_commodity, dim_market)
  3. Fact table (fact_prices)

Supports incremental loading – only inserts rows not already present.
"""

import logging
import os
import re

import pandas as pd
from sqlalchemy import create_engine, text

from config import (
    PG_CONNECTION_STRING,
    PG_SCHEMA,
    SQL_DIR,
)

logger = logging.getLogger(__name__)


def get_engine():
    """Create and return a SQLAlchemy engine for PostgreSQL."""
    engine = create_engine(PG_CONNECTION_STRING, pool_pre_ping=True)
    logger.info("PostgreSQL engine created: %s", PG_CONNECTION_STRING.split("@")[-1])
    return engine


def init_schema(engine) -> None:
    """Run the DDL script to create schema, tables, and indexes."""
    ddl_path = os.path.join(SQL_DIR, "schema.sql")
    if not os.path.exists(ddl_path):
        raise FileNotFoundError(f"Schema DDL not found: {ddl_path}")

    with open(ddl_path, "r", encoding="utf-8") as f:
        ddl = f.read()

    # Strip SQL comments (-- ...) to avoid issues when splitting on ;
    ddl_no_comments = re.sub(r"--[^\n]*", "", ddl)

    with engine.begin() as conn:
        for stmt in ddl_no_comments.split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(text(stmt))

    logger.info("Database schema initialised.")


def load_staging(df: pd.DataFrame, engine, if_exists: str = "append") -> int:
    """
    Load cleaned flat data into stg_cleaned_prices.

    Parameters
    
    df : pd.DataFrame
        Cleaned dataframe.
    engine : SQLAlchemy Engine
    if_exists : str
        'append' for incremental, 'replace' for full reload.

    Returns
    
    int
        Number of rows loaded.
    """
    staging_cols = [
        "date", "region", "county", "market", "market_id",
        "latitude", "longitude", "category", "commodity", "commodity_id",
        "unit", "price_flag", "price_type", "currency", "price", "usd_price",
        "year", "month", "price_per_kg", "usd_price_per_kg",
    ]
    df_load = df[[c for c in staging_cols if c in df.columns]].copy()
    rows = df_load.to_sql(
        "stg_cleaned_prices",
        engine,
        schema=PG_SCHEMA,
        if_exists=if_exists,
        index=False,
        method="multi",
        chunksize=5000,
    )
    loaded = rows if rows is not None else len(df_load)
    logger.info("Loaded %d rows into stg_cleaned_prices.", loaded)
    return loaded


def load_dim_date(df: pd.DataFrame, engine) -> int:
    """Populate dim_date from the cleaned dataframe."""
    dates = df[["date"]].drop_duplicates().copy()
    dates["date_id"] = dates["date"]
    dates["year"] = dates["date"].dt.year
    dates["month"] = dates["date"].dt.month
    dates["month_name"] = dates["date"].dt.strftime("%B")
    dates["quarter"] = dates["date"].dt.quarter
    dates["day_of_week"] = dates["date"].dt.dayofweek
    dates["is_weekend"] = dates["day_of_week"].isin([5, 6])

    dates = dates.drop(columns=["date"])

    # Upsert: write to temp table, then merge
    with engine.begin() as conn:
        dates.to_sql("_tmp_dim_date", conn, schema=PG_SCHEMA, if_exists="replace", index=False)
        conn.execute(text(f"""
            INSERT INTO {PG_SCHEMA}.dim_date (date_id, year, month, month_name, quarter, day_of_week, is_weekend)
            SELECT date_id, year, month, month_name, quarter, day_of_week, is_weekend
            FROM {PG_SCHEMA}._tmp_dim_date
            ON CONFLICT (date_id) DO NOTHING
        """))
        conn.execute(text(f"DROP TABLE IF EXISTS {PG_SCHEMA}._tmp_dim_date"))

    logger.info("Loaded %d unique dates into dim_date.", len(dates))
    return len(dates)


def load_dim_commodity(df: pd.DataFrame, engine) -> int:
    """Populate dim_commodity from the cleaned dataframe."""
    commodities = (
        df[["commodity_id", "commodity", "category", "unit"]]
        .drop_duplicates(subset=["commodity_id"])
        .copy()
    )

    with engine.begin() as conn:
        commodities.to_sql(
            "_tmp_dim_commodity", conn, schema=PG_SCHEMA,
            if_exists="replace", index=False,
        )
        conn.execute(text(f"""
            INSERT INTO {PG_SCHEMA}.dim_commodity (commodity_id, commodity, category, unit)
            SELECT commodity_id, commodity, category, unit
            FROM {PG_SCHEMA}._tmp_dim_commodity
            ON CONFLICT (commodity_id) DO NOTHING
        """))
        conn.execute(text(f"DROP TABLE IF EXISTS {PG_SCHEMA}._tmp_dim_commodity"))

    logger.info("Loaded %d commodities into dim_commodity.", len(commodities))
    return len(commodities)


def load_dim_market(df: pd.DataFrame, engine) -> int:
    """Populate dim_market from the cleaned dataframe."""
    markets = (
        df[["market_id", "market", "region", "county", "latitude", "longitude"]]
        .drop_duplicates(subset=["market_id"])
        .copy()
    )

    with engine.begin() as conn:
        markets.to_sql(
            "_tmp_dim_market", conn, schema=PG_SCHEMA,
            if_exists="replace", index=False,
        )
        conn.execute(text(f"""
            INSERT INTO {PG_SCHEMA}.dim_market (market_id, market, region, county, latitude, longitude)
            SELECT market_id, market, region, county, latitude, longitude
            FROM {PG_SCHEMA}._tmp_dim_market
            ON CONFLICT (market_id) DO NOTHING
        """))
        conn.execute(text(f"DROP TABLE IF EXISTS {PG_SCHEMA}._tmp_dim_market"))

    logger.info("Loaded %d markets into dim_market.", len(markets))
    return len(markets)


def load_fact_prices(df: pd.DataFrame, engine) -> int:
    """Populate fact_prices from the cleaned dataframe."""
    facts = df[[
        "date", "commodity_id", "market_id", "price_type",
        "price_flag", "currency", "price", "usd_price",
        "price_per_kg", "usd_price_per_kg",
    ]].copy()
    facts = facts.rename(columns={"date": "date_id"})

    rows = facts.to_sql(
        "fact_prices",
        engine,
        schema=PG_SCHEMA,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000,
    )
    loaded = rows if rows is not None else len(facts)
    logger.info("Loaded %d rows into fact_prices.", loaded)
    return loaded


def get_last_processed_date(engine) -> str | None:
    """
    Query the staging table for the max date already loaded.
    Used for incremental extraction.
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT MAX(date) FROM {PG_SCHEMA}.stg_cleaned_prices")
            )
            row = result.fetchone()
            if row and row[0] is not None:
                return str(row[0])
    except Exception:
        logger.info("Could not read last processed date (table may not exist yet).")
    return None


def load(df: pd.DataFrame, full_reload: bool = False) -> dict:
    """
    Orchestrate the full load into PostgreSQL.

    Parameters
    
    df : pd.DataFrame
        Cleaned & quality-checked dataframe.
    full_reload : bool
        If True, replace staging data; otherwise append.

    Returns
    
    dict
        Summary of rows loaded per table.
    """
    engine = get_engine()
    init_schema(engine)

    if full_reload:
        # Truncate staging instead of DROP+CREATE to preserve schema/indexes
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {PG_SCHEMA}.stg_cleaned_prices RESTART IDENTITY"))
            conn.execute(text(f"TRUNCATE TABLE {PG_SCHEMA}.fact_prices RESTART IDENTITY"))
        logger.info("Truncated staging and fact tables for full reload.")

    summary = {
        "staging": load_staging(df, engine, if_exists="append"),
        "dim_date": load_dim_date(df, engine),
        "dim_commodity": load_dim_commodity(df, engine),
        "dim_market": load_dim_market(df, engine),
        "fact_prices": load_fact_prices(df, engine),
    }

    logger.info("Load complete: %s", summary)
    engine.dispose()
    return summary



# Standalone test

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from extract import extract
    from clean import clean
    from quality_checks import run_quality_checks

    raw = extract(source="local")
    cleaned = clean(raw)
    report = run_quality_checks(cleaned)
    if report["overall_passed"]:
        result = load(cleaned, full_reload=True)
        print("\nLoad summary:", result)
    else:
        print("Quality checks failed. Aborting load.")
