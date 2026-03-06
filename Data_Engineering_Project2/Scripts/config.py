"""
config.py - Centralised configuration for the ETL pipeline.

Reads settings from environment variables with sensible defaults.
"""

import os

# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE", "food_prices_kenya")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")
PG_SCHEMA = os.getenv("PG_SCHEMA", "food_prices")

PG_CONNECTION_STRING = (
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}"
    f"@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
)

# ---------------------------------------------------------------------------
# Snowflake (optional)
# ---------------------------------------------------------------------------
SF_ACCOUNT = os.getenv("SF_ACCOUNT", "")
SF_USER = os.getenv("SF_USER", "")
SF_PASSWORD = os.getenv("SF_PASSWORD", "")
SF_DATABASE = os.getenv("SF_DATABASE", "FOOD_PRICES")
SF_SCHEMA = os.getenv("SF_SCHEMA", "PUBLIC")
SF_WAREHOUSE = os.getenv("SF_WAREHOUSE", "COMPUTE_WH")

# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------
DATA_SOURCE = os.getenv("DATA_SOURCE", "local")  # 'local' or 'url'
LOCAL_CSV_PATH = os.getenv(
    "LOCAL_CSV_PATH",
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "wfp_food_prices_ken.csv",
    ),
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SQL_DIR = os.path.join(PROJECT_ROOT, "SQL")
