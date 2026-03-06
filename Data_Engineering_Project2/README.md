# Project 2 — WFP Kenya Food Prices Star-Schema ETL/ELT Pipeline

> End-to-end ETL/ELT pipeline for the World Food Programme (WFP) food price monitoring dataset for Kenya. Built with Python, PostgreSQL, Apache Airflow, dbt Core, and Grafana.

> Part of the [Kenya Food Prices Data Engineering Project](../README.md). Can run standalone or as part of the unified multi-project Airflow instance.

---

## Table of Contents

1. [Problem Context](#problem-context)
2. [Architecture Overview](#architecture-overview)
3. [Project Structure](#project-structure)
4. [Setup & Installation](#setup--installation)
5. [Running the Pipeline](#running-the-pipeline)
6. [Database Schema (Star Schema)](#database-schema-star-schema)
7. [dbt Models](#dbt-models)
8. [Airflow DAG](#airflow-dag)
9. [Grafana Dashboards](#grafana-dashboards)
10. [Snowflake (Optional)](#snowflake-optional)
11. [Design Choices & Assumptions](#design-choices--assumptions)
12. [Challenges & Learnings](#challenges--learnings)
13. [Future Ideas](#future-ideas)

---

## Problem Context

Food security in Kenya is a critical concern, with staple food prices directly affecting millions of households. The **World Food Programme (WFP)** collects market price data across Kenyan counties for essential commodities (maize, beans, milk, cooking oil, etc.).

This project builds a **production-grade data pipeline** that:

- **Extracts** WFP food price data (17,600+ records from 2006–2025)
- **Transforms** raw data with pandas (cleaning, standardisation, enrichment)
- **Validates** data quality with automated checks
- **Loads** into a PostgreSQL star schema for analytical queries
- **Models** aggregate views with dbt Core
- **Visualises** trends in Grafana dashboards
- **Orchestrates** everything with Apache Airflow

---

## Architecture Overview

```
┌──────────────┐     ┌──────────────┐     ┌──────────────────┐
│  WFP CSV     │────>│  extract.py  │────>│   clean.py       │
│  (local/URL) │     │  (requests)  │     │  (pandas)        │
└──────────────┘     └──────────────┘     └────────┬─────────┘
                                                   │
                                          ┌────────▼─────────┐
                                          │ quality_checks.py │
                                          └────────┬─────────┘
                                                   │
                     ┌─────────────────────────────▼──────────┐
                     │              load.py                     │
                     │  ┌─────────┐ ┌───────────┐ ┌─────────┐ │
                     │  │dim_date │ │dim_market  │ │dim_comm.│ │
                     │  └────┬────┘ └─────┬─────┘ └────┬────┘ │
                     │       └────────────┼────────────┘      │
                     │              ┌─────▼─────┐              │
                     │              │fact_prices │              │
                     │              └───────────┘              │
                     └─────────────────────────────────────────┘
                                          │
                     ┌────────────────────▼────────────────────┐
                     │          dbt Core Models                 │
                     │  • agg_monthly_avg_price                │
                     │  • agg_price_trends                     │
                     │  • agg_commodity_summary                │
                     └────────────────────┬────────────────────┘
                                          │
                     ┌────────────────────▼────────────────────┐
                     │        Grafana Dashboards                │
                     │  • Maize price trend line                │
                     │  • Avg price by commodity bar chart      │
                     │  • Observations by county                │
                     └─────────────────────────────────────────┘

   Orchestration: Apache Airflow DAG (daily schedule)
```

---

## Project Structure

```
Data_Engineering_Project2/
├── wfp_food_prices_ken.csv          # Source dataset
├── docker-compose.yml               # PostgreSQL + Airflow + Grafana
├── requirements.txt                 # Python dependencies
├── .env.example                     # Environment variable template
├── .gitignore
├── README.md
│
├── Scripts/                         # Modular Python ETL code
│   ├── __init__.py
│   ├── config.py                    # Centralised configuration
│   ├── extract.py                   # Data extraction (URL / local)
│   ├── clean.py                     # Pandas cleaning & enrichment
│   ├── quality_checks.py           # Data quality validation
│   ├── load.py                      # PostgreSQL loader (star schema)
│   ├── pipeline.py                  # Main ETL orchestrator (CLI)
│   └── snowflake_load.py           # Optional Snowflake module
│
├── SQL/
│   └── schema.sql                   # DDL: staging + star schema
│
├── dags/
│   └── wfp_food_prices_dag.py      # Airflow DAG definition
│
├── dbt/
│   └── food_prices_dbt/
│       ├── dbt_project.yml
│       ├── profiles.yml
│       └── models/
│           ├── sources.yml
│           ├── schema.yml
│           ├── agg_monthly_avg_price.sql
│           ├── agg_price_trends.sql
│           └── agg_commodity_summary.sql
│
└── grafana/
    ├── provisioning/
    │   ├── datasources/postgres.yml
    │   └── dashboards/dashboards.yml
    └── dashboards/
        └── food_prices_dashboard.json
```

---

## Setup & Installation

### Prerequisites

- **Python 3.11+**
- **PostgreSQL 17** (local or Docker)
- **Docker Engine** + Docker Compose
- **Apache Airflow 3.x** (via Docker image or local)

### 1. Clone / Unzip

```bash
cd ~/Data_Engineering_Project2
```

### 2. Python Virtual Environment

```bash
python -m venv venv
venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

### 3. Start Infrastructure (Docker)

```bash
docker-compose up -d
```

This starts:
| Service | Port (Standalone) | Port (Unified) | Credentials |
|---------|-------------------|----------------|-------------|
| PostgreSQL (data) | 5432 | 5434 | postgres / postgres |
| PostgreSQL (Airflow) | 5433 | 5433 | airflow / airflow |
| Airflow API Server | 8080 | 8080 | auto (all admins) |
| Grafana | 3000 | 3000 | admin / admin |

### 4. Create the Database (if not using Docker init)

```sql
CREATE DATABASE food_prices_kenya;
```

Then run the schema:

```bash
psql -U postgres -d food_prices_kenya -f SQL/schema.sql
```

### 5. Install dbt

```bash
pip install dbt-core dbt-postgres
```

---

## Running the Pipeline

### Option A: CLI (standalone)

```bash
cd Scripts

# Full reload
python pipeline.py --source local --full-reload

# Incremental (default)
python pipeline.py --source local

# Dry-run (no DB load)
python pipeline.py --dry-run
```

### Option B: Airflow DAG

1. Open Airflow UI: `http://localhost:8080`
2. Enable the `wfp_kenya_food_prices_etl` DAG
3. Trigger manually or let it run on the daily schedule

### Option C: dbt

```bash
cd dbt/food_prices_dbt
dbt run --profiles-dir . --project-dir .
dbt test --profiles-dir . --project-dir .
```

---

## Database Schema (Star Schema)

### Staging Table
- `stg_cleaned_prices` — flat cleaned data (all columns)

### Dimension Tables
| Table | Key | Description |
|-------|-----|-------------|
| `dim_date` | `date_id` (DATE) | Calendar attributes: year, month, quarter |
| `dim_commodity` | `commodity_id` (INT) | Commodity name, category, unit |
| `dim_market` | `market_id` (INT) | Market name, county, district, lat/lon |

### Fact Table
| Table | Description |
|-------|-------------|
| `fact_prices` | Price observations linking to all three dimensions |

---

## dbt Models

| Model | Type | Description |
|-------|------|-------------|
| `agg_monthly_avg_price` | View | Monthly avg price per county & commodity |
| `agg_price_trends` | View | Year-over-year price change per commodity |
| `agg_commodity_summary` | View | Overall statistics per commodity |

All models include dbt tests (not_null, unique) defined in `schema.yml`.

---

## Airflow DAG

**DAG ID:** `wfp_kenya_food_prices_etl`

**Tasks:**
1. `extract_data` — Load CSV (with incremental date filter)
2. `clean_data` — Pandas cleaning pipeline
3. `quality_check` — Validate data quality
4. `load_to_postgres` — Load staging + star schema
5. `run_dbt` — Build dbt aggregate models

**Schedule:** `@daily` (configurable)

---

## Grafana Dashboards

Three pre-configured panels (auto-provisioned):

1. **Maize Price Trend Line** — Monthly average retail maize price over time
2. **Average USD Price by Commodity** — Top 15 commodities bar chart
3. **Observations by County** — Data coverage across Kenyan counties

Access at `http://localhost:3000` (admin/admin).

---

## Snowflake (Optional)

The `snowflake_load.py` module demonstrates:
- Loading the same cleaned data to Snowflake
- A **query syntax difference**: Snowflake's `QUALIFY` clause (filters on window functions inline) vs PostgreSQL's CTE/subquery approach

To enable: set `SF_ACCOUNT`, `SF_USER`, `SF_PASSWORD` environment variables.

---

## Design Choices & Assumptions

| Decision | Rationale |
|----------|-----------|
| **Star schema** over flat table | Enables efficient analytical queries, joins are predictable |
| **Incremental loading** | Avoids re-processing historical data; uses max(date) as watermark |
| **price_per_kg normalisation** | Different units (KG, 90 KG, 400 G, etc.) make raw prices incomparable; normalising to per-kg enables cross-commodity analysis |
| **Unit → KG mapping** | Approximations: 1L ≈ 1kg for liquids; "Unit", "Bunch", "Head" cannot be converted → NULL |
| **admin1 → county, admin2 → district** | Standard Kenyan administrative naming convention |
| **Missing lat/lon** | Forward-filled within market groups; 47 rows affected |
| **dbt views** (not tables) | For a dataset this size, views are performant and always up-to-date |
| **Docker Compose** | Single-command infrastructure setup for reproducibility |
| **Parquet for Airflow XCom** | Efficient serialisation between Airflow tasks |

---

## Challenges & Learnings

- **Unit heterogeneity**: The dataset uses 14 different units, requiring a carefully maintained conversion table
- **Data quality issues**: 47 rows missing geographic info → handled with intelligent fill strategies
- **Incremental logic**: Implementing date-based watermarks required coordination between extract and load modules
- **dbt + raw schema**: Connecting dbt to an existing Postgres schema (not dbt-managed) required explicit source definitions

---

## Future Ideas

- **Real-time ingestion** from WFP API with Apache Kafka / Flink
- **ML forecasting** for food price prediction (ARIMA / Prophet)
- **Geospatial analysis** using PostGIS for market proximity clustering
- **Alerting** in Grafana for price spikes above thresholds
- **Data lineage** with OpenLineage / Marquez integration
- **CI/CD** pipeline with GitHub Actions for automated dbt tests

---

## Presentation Outline (10-15 min)

1. **Problem Context** (2 min) — Food prices in Kenya, WFP monitoring
2. **Architecture Diagram** (2 min) — Walk through the pipeline diagram above
3. **Key Code Snippets** (3 min) — extract.py, clean.py price_per_kg logic, SQL star schema
4. **Live Demo** (5 min) — Run Airflow DAG, show Grafana visuals, dbt run output
5. **Challenges & Learnings** (2 min)
6. **Future Ideas** (1 min)

---

*Built as part of a Data Engineering curriculum project.*
