"""
pipeline.py - Main ETL orchestrator for WFP Food Prices Kenya.

Ties together:  extract → clean → quality_checks → load

Supports:
  - Full reload (all data)
  - Incremental mode (only new dates since last load)
  - Dry-run (extract + clean + check, but skip load)
"""

import argparse
import logging
import sys

from extract import extract
from clean import clean
from quality_checks import run_quality_checks, QualityCheckError
from load import load, get_engine, get_last_processed_date

logger = logging.getLogger(__name__)


def run_pipeline(
    source: str = "local",
    incremental: bool = True,
    full_reload: bool = False,
    dry_run: bool = False,
) -> dict:
    """
    Execute the full ETL pipeline.

    Parameters
    
    source : str
        'local' or 'url'.
    incremental : bool
        If True, only process rows newer than the last loaded date.
    full_reload : bool
        If True, replace all data in staging (overrides incremental).
    dry_run : bool
        If True, skip the database load step.

    Returns
    
    dict
        Pipeline execution summary.
    """
    summary = {"status": "started"}

    # 1. Determine incremental cut-off
    last_date = None
    if incremental and not full_reload:
        try:
            engine = get_engine()
            last_date = get_last_processed_date(engine)
            engine.dispose()
            if last_date:
                logger.info("Incremental mode: loading data after %s", last_date)
        except Exception as exc:
            logger.info("No previous load detected (%s). Running full load.", exc)

    # 2. Extract
    logger.info("=== EXTRACT ===")
    df_raw = extract(source=source, last_processed_date=last_date)
    summary["rows_extracted"] = len(df_raw)

    if df_raw.empty:
        logger.info("No new data to process. Pipeline complete.")
        summary["status"] = "no_new_data"
        return summary

    # 3. Clean / Transform
    logger.info("=== TRANSFORM ===")
    df_clean = clean(df_raw)
    summary["rows_cleaned"] = len(df_clean)

    # 4. Quality Checks ---
    logger.info("=== QUALITY CHECKS ===")
    try:
        qc_report = run_quality_checks(df_clean)
        summary["quality_report"] = qc_report
    except QualityCheckError as exc:
        logger.error("Critical quality check failed: %s", exc)
        summary["status"] = "quality_check_failed"
        return summary

    # 5. Load
    if dry_run:
        logger.info("Dry-run mode — skipping database load.")
        summary["status"] = "dry_run_complete"
    else:
        logger.info("=== LOAD ===")
        load_summary = load(df_clean, full_reload=full_reload)
        summary["load"] = load_summary
        summary["status"] = "success"

    logger.info("Pipeline finished. Status: %s", summary["status"])
    return summary



# CLI entry point

def main():
    parser = argparse.ArgumentParser(description="WFP Kenya Food Prices ETL Pipeline")
    parser.add_argument(
        "--source", choices=["local", "url"], default="local",
        help="Data source: 'local' CSV file or 'url' download.",
    )
    parser.add_argument(
        "--full-reload", action="store_true",
        help="Replace all staging data instead of appending.",
    )
    parser.add_argument(
        "--no-incremental", action="store_true",
        help="Disable incremental mode (load all data).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Run extract/clean/check but skip database load.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    result = run_pipeline(
        source=args.source,
        incremental=not args.no_incremental,
        full_reload=args.full_reload,
        dry_run=args.dry_run,
    )
    print("\n=== Pipeline Summary ===")
    for k, v in result.items():
        print(f"  {k}: {v}")

    sys.exit(0 if result["status"] in ("success", "dry_run_complete", "no_new_data") else 1)


if __name__ == "__main__":
    main()
