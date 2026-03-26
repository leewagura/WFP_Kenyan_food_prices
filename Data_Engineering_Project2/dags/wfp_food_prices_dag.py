"""
wfp_food_prices_dag.py - Apache Airflow DAG for WFP Kenya Food Prices ETL.

Schedule: daily 
Tasks:
  1. extract_data  – download / load CSV
  2. clean_data    – pandas transformations
  3. quality_check – validate cleaned data
  4. load_to_postgres – load staging + star schema
  5. run_dbt       – build dbt aggregate models

Compatible with Airflow 2.x / 3.x (TaskFlow API).
"""

from datetime import datetime, timedelta
import logging
import os
import sys

# Add the Scripts directory to sys.path so Airflow workers can import ETL modules
_SCRIPTS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "Scripts"
)
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from airflow.decorators import dag, task  # noqa: E402  # type: ignore[import-untyped]
from airflow.operators.bash import BashOperator  # noqa: E402  # type: ignore[import-untyped]

import extract as extract_mod  # noqa: E402  # type: ignore[import-not-found]
import clean as clean_mod  # noqa: E402  # type: ignore[import-not-found]
import quality_checks as qc_mod  # noqa: E402  # type: ignore[import-not-found]
import load as load_mod  # noqa: E402  # type: ignore[import-not-found]

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="wfp_kenya_food_prices_etl",
    description="ETL pipeline for WFP Kenya food price data → PostgreSQL star schema",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["wfp", "kenya", "food_prices", "etl"],
)
def wfp_food_prices_pipeline():

    @task()
    def extract_data(**context):
        """Extract raw data from local CSV or URL."""
        # Attempt incremental logic
        last_date = None
        try:
            engine = load_mod.get_engine()
            last_date = load_mod.get_last_processed_date(engine)
            engine.dispose()
        except Exception:
            logger.info("No previous load found. Running full extraction.")

        df = extract_mod.extract(source="url", last_processed_date=last_date)
        logger.info("Extracted %d rows.", len(df))

        if df.empty:
            logger.info("No new data to process. Skipping pipeline.")
            return ""

        # Serialize to temp parquet for downstream tasks
        tmp_path = "/tmp/wfp_raw.parquet"
        df.to_parquet(tmp_path, index=False)
        return tmp_path

    @task()
    def clean_data(raw_path: str, **context):
        """Clean and transform the extracted data."""
        if not raw_path:
            logger.info("No new data. Skipping clean.")
            return ""

        import pandas as pd

        df = pd.read_parquet(raw_path)
        df_clean = clean_mod.clean(df)
        logger.info("Cleaned data: %d rows.", len(df_clean))

        tmp_path = "/tmp/wfp_cleaned.parquet"
        df_clean.to_parquet(tmp_path, index=False)
        return tmp_path

    @task()
    def quality_check(cleaned_path: str, **context):
        """Run quality checks on cleaned data."""
        if not cleaned_path:
            logger.info("No new data. Skipping quality check.")
            return ""

        import pandas as pd

        df = pd.read_parquet(cleaned_path)
        report = qc_mod.run_quality_checks(df)
        logger.info("Quality report: %s", report)

        if not report.get("overall_passed", False):
            raise ValueError(f"Quality checks failed: {report}")

        return cleaned_path

    @task()
    def load_to_postgres(cleaned_path: str, **context):
        """Load cleaned data into PostgreSQL staging + star schema."""
        if not cleaned_path:
            logger.info("No new data. Skipping load.")
            return {"status": "skipped", "reason": "no new data"}

        import pandas as pd

        df = pd.read_parquet(cleaned_path)
        summary = load_mod.load(df, full_reload=False)
        logger.info("Load summary: %s", summary)
        return summary

    # dbt run (BashOperator for simplicity)
    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command=(
            "cd /opt/airflow/dbt/food_prices_dbt "
            "&& dbt run --profiles-dir . --project-dir ."
        ),
    )

    # Task dependencies (linear chain)
    raw_path = extract_data()
    cleaned_path = clean_data(raw_path)
    checked_path = quality_check(cleaned_path)
    load_result = load_to_postgres(checked_path)
    load_result >> run_dbt  # type: ignore


# Instantiate the DAG
wfp_food_prices_pipeline()
