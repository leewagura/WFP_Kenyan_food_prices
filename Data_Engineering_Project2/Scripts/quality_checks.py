"""
quality_checks.py - Data quality validation module.

Runs a battery of checks on the cleaned DataFrame before loading.
Returns a report dict; raises on critical failures.
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)


class QualityCheckError(Exception):
    """Raised when a critical quality check fails."""


def check_not_empty(df: pd.DataFrame) -> bool:
    """Fail if the DataFrame is empty."""
    if df.empty:
        raise QualityCheckError("DataFrame is empty after cleaning.")
    logger.info("✓ Non-empty check passed (%d rows).", len(df))
    return True


def check_no_duplicate_rows(df: pd.DataFrame) -> dict:
    """Detect full-row duplicates."""
    key_cols = ["date", "market_id", "commodity_id", "price_type"]
    existing = [c for c in key_cols if c in df.columns]
    dups = df.duplicated(subset=existing, keep=False).sum()
    result = {"duplicates": int(dups), "passed": dups == 0}
    if dups:
        logger.warning("⚠ Found %d duplicate rows on key columns.", dups)
    else:
        logger.info("✓ No duplicate rows on key columns.")
    return result


def check_date_range(df: pd.DataFrame) -> dict:
    """Verify dates are within a plausible range (2000–2030)."""
    min_date = df["date"].min()
    max_date = df["date"].max()
    in_range = min_date >= pd.Timestamp("2000-01-01") and max_date <= pd.Timestamp("2030-12-31")
    result = {"min_date": str(min_date.date()), "max_date": str(max_date.date()), "passed": in_range}
    if in_range:
        logger.info("✓ Date range check passed: %s → %s", min_date.date(), max_date.date())
    else:
        logger.warning("⚠ Dates outside expected range: %s → %s", min_date.date(), max_date.date())
    return result


def check_positive_prices(df: pd.DataFrame) -> dict:
    """Ensure all prices are positive."""
    neg_prices = (df["price"] <= 0).sum()
    neg_usd = (df["usd_price"] <= 0).sum()
    passed = neg_prices == 0 and neg_usd == 0
    result = {"negative_prices": int(neg_prices), "negative_usd_prices": int(neg_usd), "passed": passed}
    if passed:
        logger.info("✓ All prices are positive.")
    else:
        logger.warning("⚠ Found %d non-positive prices, %d non-positive USD prices.", neg_prices, neg_usd)
    return result


def check_null_critical_fields(df: pd.DataFrame) -> dict:
    """Check that critical columns have no nulls."""
    critical = ["date", "market", "commodity", "price", "usd_price"]
    null_counts = {col: int(df[col].isna().sum()) for col in critical if col in df.columns}
    total_nulls = sum(null_counts.values())
    result = {"null_counts": null_counts, "passed": total_nulls == 0}
    if total_nulls == 0:
        logger.info("✓ No nulls in critical fields.")
    else:
        logger.warning("⚠ Nulls in critical fields: %s", null_counts)
    return result


def check_price_outliers(df: pd.DataFrame, z_threshold: float = 5.0) -> dict:
    """Flag extreme price outliers using z-score on price_per_kg."""
    if "price_per_kg" not in df.columns or df["price_per_kg"].isna().all():
        return {"outliers": 0, "passed": True, "skipped": True}

    ppkg = df["price_per_kg"].dropna()
    mean, std = ppkg.mean(), ppkg.std()
    if std == 0:
        return {"outliers": 0, "passed": True}

    z = ((ppkg - mean) / std).abs()
    outlier_count = int((z > z_threshold).sum())
    result = {"outliers": outlier_count, "threshold": z_threshold, "passed": outlier_count < len(df) * 0.01}
    if result["passed"]:
        logger.info("✓ Outlier check passed (%d outliers, <1%% of data).", outlier_count)
    else:
        logger.warning("⚠ %d price outliers detected (>1%% of data).", outlier_count)
    return result


def run_quality_checks(df: pd.DataFrame) -> dict:
    """
    Execute all quality checks and return a consolidated report.

    Parameters
    
    df : pd.DataFrame
        Cleaned dataframe.

    Returns
    
    dict
        Report with individual check results and overall pass/fail.

    Raises
    
    QualityCheckError
        If a critical check fails (e.g. empty dataframe).
    """
    logger.info("Running quality checks on %d rows...", len(df))
    report = {}

    check_not_empty(df)
    report["duplicates"] = check_no_duplicate_rows(df)
    report["date_range"] = check_date_range(df)
    report["positive_prices"] = check_positive_prices(df)
    report["null_critical"] = check_null_critical_fields(df)
    report["outliers"] = check_price_outliers(df)

    overall = all(v.get("passed", True) for v in report.values())
    report["overall_passed"] = overall

    if overall:
        logger.info("All quality checks PASSED.")
    else:
        logger.warning("Some quality checks FAILED. Review report.")

    return report



# Standalone test

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from extract import extract
    from clean import clean

    raw = extract(source="local")
    cleaned = clean(raw)
    report = run_quality_checks(cleaned)
    print("\nQuality Report:")
    for k, v in report.items():
        print(f"  {k}: {v}")
