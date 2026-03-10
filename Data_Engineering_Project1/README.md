# Project 1 — Kenya Food Prices SQL-First ETL Pipeline

An Apache Airflow 3.1.7 pipeline that loads WFP (World Food Programme) Kenya food-price data into PostgreSQL and produces aggregate summary tables using pure SQL transformations.

> Part of the [Kenya Food Prices Data Engineering Project](../README.md). Can run standalone or as part of the unified multi-project Airflow instance.

## Architecture

| Component | Detail |
|---|---|
| Orchestrator | Apache Airflow 3.1.7 (Docker Compose) |
| Database | PostgreSQL 17 (`postgres_project1`, port 5435) |
| Source file | `wfp_food_prices_ken.csv` — 17,632 records (2006–2025) |

## DAG: `kenya_food_prices_pipeline`

```
create_staging_table → load_csv_to_staging → clean_data → ┌─ aggregate_avg_prices
                                                          └─ aggregate_avg_price_by_category
```

| Task | Type | Description |
|---|---|---|
| `create_staging_table` | SQLExecuteQueryOperator | Creates `raw_food_prices` table if it does not exist |
| `load_csv_to_staging` | PythonOperator | Truncates and bulk-inserts all 17,632 CSV rows (idempotent) |
| `clean_data` | SQLExecuteQueryOperator | Creates `cleaned_food_prices` by dropping nulls and duplicates from the staging table |
| `aggregate_avg_prices` | SQLExecuteQueryOperator | Builds `avg_price_per_commodity_year` from **cleaned** data |
| `aggregate_avg_price_by_category` | SQLExecuteQueryOperator | Builds `avg_price_per_category` from **cleaned** data |

## Output Tables

### `raw_food_prices` (17,632 rows)

| Column | Type | Description |
|--------|------|-------------|
| date | DATE | Price observation date |
| admin1 | VARCHAR(50) | Province/region |
| admin2 | VARCHAR(50) | County/district |
| market | VARCHAR(100) | Market name |
| latitude / longitude | NUMERIC | GPS coordinates |
| category | VARCHAR(50) | Food category |
| commodity | VARCHAR(100) | Commodity name |
| price | NUMERIC(8,2) | Price in KES |
| usdprice | NUMERIC(8,2) | Price in USD |

### `avg_price_per_commodity_year`

| Column | Description |
|--------|-------------|
| price_year | Year extracted from date |
| commodity | Commodity name |
| pricetype | Wholesale / Retail |
| num_observations | Count of price records |
| avg_price_kes | Average price in KES |
| avg_price_usd | Average price in USD |

### `avg_price_per_category`

| Column | Description |
|--------|-------------|
| category | Food category |
| num_commodities | Distinct commodities in category |
| num_observations | Total price records |
| avg_price_kes / avg_price_usd | Average prices |
| min_price_kes / max_price_kes | Price range |

## Quick Start

### Standalone (this project only)

```bash
cd Data_Engineering_Project1
docker compose up -d
```

### Unified (both projects)

```bash
cd ..   # parent directory
docker compose up -d
```

Open Airflow at **http://localhost:8080** and trigger `kenya_food_prices_pipeline`.

## Connection

| Field | Value |
|-------|-------|
| Conn ID | `my_postgres_db` |
| Host | `postgres_project1` (Docker) / `localhost` (pgAdmin) |
| Port | 5432 (Docker) / 5435 (pgAdmin) |
| Database | `airflow` |
| User / Password | `airflow` / `airflow` |

## Data Quality Issues Observed

The following issues were identified in `wfp_food_prices_ken.csv`:

### 1. Missing Location Metadata (47 rows)

47 records have **NULL values** in `admin1`, `admin2`, `latitude`, and `longitude`. These rows have a valid `market` name and `market_id` but no region or GPS coordinates, which limits any geographic analysis for those observations.

### 2. Commodity Naming Inconsistencies

Several commodity groups use overlapping or ambiguous names that make aggregation unreliable without manual mapping:

| Group | Variants in the dataset |
|---|---|
| Maize | `Maize`, `Maize (white)`, `Maize (white, dry)`, `Maize flour`, `Maize flour (white)` |
| Beans | `Beans`, `Beans (dry)`, `Beans (kidney)`, `Beans (yellow)`, `Beans (dolichos)`, `Beans (mung)`, `Beans (rosecoco)` |
| Sorghum | `Sorghum`, `Sorghum (red)`, `Sorghum (white)` |
| Potatoes | `Potatoes (Irish)`, `Potatoes (Irish, red)`, `Potatoes (Irish, white)` |

For example, `Maize` and `Maize (white)` may refer to the same product but are treated as separate commodities, inflating distinct-commodity counts and splitting price trends.

### 3. Inconsistent `priceflag` Values

The `priceflag` column contains three distinct values:

- `actual` — directly observed price
- `aggregate` — computed/aggregated price
- `actual,aggregate` — a hybrid value that combines both flags in a single comma-separated string

The `actual,aggregate` value breaks any simple filter such as `WHERE priceflag = 'actual'` and needs to be split or standardised before analysis.

## Project Files

| File | Purpose |
|---|---|
| `docker-compose.yaml` | Airflow 3.1.7 + PostgreSQL 17 stack (uses `.env` for secrets) |
| `.env.example` | Template for required environment variables |
| `dags/kenya_food_prices_pipeline.py` | Airflow DAG (5 tasks, including data cleaning) |
| `wfp_food_prices_ken.csv` | Raw WFP dataset |
| `food_prices.sql` | Standalone SQL: table DDL + example analytic queries |
| `load_raw_food_prices.py` | Standalone Python loader (reads credentials from env vars) |
| `clean1.py` | Standalone data cleaning script: fixes dtypes, drops nulls & duplicates |
