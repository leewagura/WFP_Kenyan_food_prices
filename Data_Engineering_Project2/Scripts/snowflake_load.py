"""
snowflake_load.py - Snowflake loading module.

Demonstrates loading the same cleaned data to Snowflake and highlights
a query difference vs PostgreSQL.

Requirements:
    pip install snowflake-connector-python snowflake-sqlalchemy
"""

import logging
import pandas as pd
from sqlalchemy import create_engine, text

from config import (
    SF_ACCOUNT,
    SF_USER,
    SF_PASSWORD,
    SF_DATABASE,
    SF_SCHEMA,
    SF_WAREHOUSE,
)

logger = logging.getLogger(__name__)


def get_snowflake_engine():
    """Create a SQLAlchemy engine for Snowflake."""
    if not all([SF_ACCOUNT, SF_USER, SF_PASSWORD]):
        raise EnvironmentError(
            "Snowflake credentials not set. Export SF_ACCOUNT, SF_USER, SF_PASSWORD."
        )
    conn_str = (
        f"snowflake://{SF_USER}:{SF_PASSWORD}@{SF_ACCOUNT}/"
        f"{SF_DATABASE}/{SF_SCHEMA}?warehouse={SF_WAREHOUSE}"
    )
    return create_engine(conn_str)


def create_snowflake_tables(engine) -> None:
    """Create staging + fact table in Snowflake."""
    ddl = """
    CREATE TABLE IF NOT EXISTS stg_cleaned_prices (
        date            DATE,
        region          VARCHAR(80),
        county          VARCHAR(80),
        market          VARCHAR(120),
        market_id       INT,
        latitude        FLOAT,
        longitude       FLOAT,
        category        VARCHAR(80),
        commodity       VARCHAR(120),
        commodity_id    INT,
        unit            VARCHAR(20),
        price_flag      VARCHAR(30),
        price_type      VARCHAR(20),
        currency        VARCHAR(10),
        price           FLOAT,
        usd_price       FLOAT,
        year            INT,
        month           INT,
        price_per_kg    FLOAT,
        usd_price_per_kg FLOAT,
        loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
    logger.info("Snowflake tables created.")


def load_to_snowflake(df: pd.DataFrame) -> int:
    """Load cleaned DataFrame to Snowflake staging table."""
    engine = get_snowflake_engine()
    create_snowflake_tables(engine)

    rows = df.to_sql(
        "stg_cleaned_prices",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000,
    )
    loaded = rows if rows is not None else len(df)
    logger.info("Loaded %d rows to Snowflake.", loaded)
    engine.dispose()
    return loaded


def demo_query_difference():
    """
    Show one query syntax difference between PostgreSQL and Snowflake.

    PostgreSQL uses DATE_TRUNC('month', date) while Snowflake can also use
    DATE_TRUNC('MONTH', date) but additionally supports FLATTEN, VARIANT
    types, and other Snowflake-specific features.
    """
    pg_query = """
    -- PostgreSQL: Monthly average price per commodity
    SELECT
        DATE_TRUNC('month', date_id)::DATE  AS price_month,
        c.commodity,
        ROUND(AVG(f.usd_price), 4)         AS avg_usd_price
    FROM food_prices.fact_prices f
    JOIN food_prices.dim_commodity c ON f.commodity_id = c.commodity_id
    GROUP BY 1, 2
    ORDER BY 1, 2;
    """

    sf_query = """
    -- Snowflake: Monthly average price per commodity
    -- Snowflake supports QUALIFY for window-function filtering (not in PG)
    SELECT
        DATE_TRUNC('MONTH', date)           AS price_month,
        commodity,
        ROUND(AVG(usd_price), 4)            AS avg_usd_price,
        RANK() OVER (
            PARTITION BY DATE_TRUNC('MONTH', date)
            ORDER BY AVG(usd_price) DESC
        )                                   AS price_rank
    FROM stg_cleaned_prices
    GROUP BY 1, 2
    QUALIFY price_rank <= 5
    ORDER BY 1, price_rank;
    """
    print("=" * 60)
    print("QUERY DIFFERENCE: PostgreSQL vs Snowflake")
    print("=" * 60)
    print("\n--- PostgreSQL ---")
    print(pg_query)
    print("\n--- Snowflake (uses QUALIFY clause – PG doesn't support it) ---")
    print(sf_query)
    print(
        "\nKey difference: Snowflake's QUALIFY clause lets you filter on "
        "window functions directly, whereas PostgreSQL requires a subquery/CTE."
    )



if __name__ == "__main__":
    demo_query_difference()
