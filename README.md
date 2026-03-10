# Kenya Food Prices — Data Engineering Project

> Two complementary data pipelines analysing **World Food Programme (WFP)** food price data for Kenya (17,632 records, 2006–2025). Orchestrated by a **single Apache Airflow 3.1.7** instance managing both pipelines through a unified Docker Compose setup.

---

## Architecture

```
                        ┌──────────────────────────────────────────┐
                        │          Apache Airflow 3.1.7            │
                        │  ┌────────────────┐ ┌────────────────┐  │
                        │  │   Project 1     │ │   Project 2     │  │
                        │  │   DAG (ETL)     │ │   DAG (ELT)     │  │
                        │  └───────┬────────┘ └───────┬────────┘  │
                        └──────────┼──────────────────┼───────────┘
                                   │                  │
                    ┌──────────────▼──┐     ┌────────▼───────────┐
                    │  PostgreSQL 17   │     │  PostgreSQL 17      │
                    │  (Project 1 DW)  │     │  (Project 2 DW)     │
                    │  Port 5435       │     │  Port 5434           │
                    │                  │     │                      │
                    │  raw_food_prices │     │  Star Schema:        │
                    │  avg_*_year      │     │  ├─ dim_date         │
                    │  avg_*_category  │     │  ├─ dim_commodity    │
                    └──────────────────┘     │  ├─ dim_market       │
                                             │  ├─ fact_prices      │
                           ┌─────────────┐   │  └─ stg_cleaned     │
                           │ Grafana     │   └──────────────────────┘
                           │ Port 3000   │──────────────┘
                           └─────────────┘
                                             ┌──────────────────────┐
                                             │  Airflow Metadata DB  │
                                             │  Port 5433            │
                                             └──────────────────────┘
```

### Services (10 containers)

| Container | Image | Port | Purpose |
|-----------|-------|------|---------|
| `postgres_airflow` | postgres:17 | 5433 | Airflow metadata DB |
| `postgres_project1` | postgres:17 | 5435 | Project 1 data warehouse |
| `postgres_project2` | postgres:17 | 5434 | Project 2 star schema DB |
| `airflow-init` | airflow:3.1.7 | — | DB migrations (one-shot) |
| `airflow-webserver` | airflow:3.1.7 | 8080 | Airflow UI (api-server) |
| `airflow-scheduler` | airflow:3.1.7 | — | Task scheduling + execution |
| `airflow-dag-processor` | airflow:3.1.7 | — | DAG file parsing |
| `airflow-triggerer` | airflow:3.1.7 | — | Deferred task handling |
| `grafana` | grafana:11.1.0 | 3000 | Dashboard visualisation |

---

## Project 1 — SQL-First ETL Pipeline

**DAG:** `kenya_food_prices_pipeline` · 4 tasks · [Details →](Data_Engineering_Project1/README.md)

Approach: Pure SQL transformations using Airflow's `SQLExecuteQueryOperator`.

```
create_staging_table → load_csv_to_staging → clean_data → ┬─ aggregate_avg_prices
                                                           └─ aggregate_avg_price_by_category
```

| What it does | How |
|---|---|
| Creates `raw_food_prices` table | `CREATE TABLE IF NOT EXISTS` |
| Loads 17,632 CSV rows | Python + `PostgresHook` (truncate-and-reload, idempotent) |
| Cleans data (nulls + duplicates) | SQL: `CREATE TABLE cleaned_food_prices AS SELECT DISTINCT … WHERE … IS NOT NULL` |
| Avg price per commodity per year | `CREATE TABLE AS SELECT … GROUP BY year, commodity` (from cleaned data) |
| Avg price per category | `CREATE TABLE AS SELECT … GROUP BY category` (from cleaned data) |

**Sample output — avg price per category:**

| Category | Commodities | Observations | Avg KES | Avg USD |
|---|---|---|---|---|
| Pulses and Nuts | 10 | 3,208 | 3,483.35 | 31.70 |
| Vegetables and Fruits | 7 | 2,058 | 1,502.90 | 12.58 |
| Cereals and Tubers | 17 | 7,851 | 1,121.12 | 10.74 |
| Meat, Fish and Eggs | 4 | 623 | 554.65 | 4.51 |
| Oil and Fats | 3 | 1,109 | 256.65 | 2.08 |
| Milk and Dairy | 4 | 970 | 100.66 | 0.80 |

---

## Project 2 — Python + dbt ELT Pipeline

**DAG:** `wfp_kenya_food_prices_etl` · 5 tasks · [Details →](Data_Engineering_Project2/README.md)

Approach: Modular Python (extract → clean → validate → load) + dbt Core for aggregate models + Grafana dashboards.

```
extract_data → clean_data → quality_check → load_to_postgres → run_dbt
```

| What it does | How |
|---|---|
| Extract CSV (incremental) | `extract.py` — date-based watermark filtering |
| Clean & enrich | `clean.py` — pandas: column renaming, price_per_kg normalisation, type casting |
| Quality checks | `quality_checks.py` — not-empty, no-duplicates, value range validation |
| Load star schema | `load.py` — staging + 3 dimensions + 1 fact table |
| dbt aggregates | 3 models: monthly avg, price trends, commodity summary |

**Star Schema:**

| Table | Rows | Description |
|---|---|---|
| `fact_prices` | 17,632 | Price observations with foreign keys to all dimensions |
| `dim_commodity` | 50 | Commodity name, category, unit |
| `dim_market` | 225 | Market, county, district, lat/lon |
| `dim_date` | 239 | Year, month, quarter, day_of_week |
| `stg_cleaned_prices` | 17,632 | Flat cleaned staging table |

---

## Quick Start

### Prerequisites

- Docker Engine + Docker Compose
- ~4 GB free disk space (Docker images)

### 1. Clone

```bash
git clone https://github.com/<your-username>/kenya-food-prices-pipeline.git
cd kenya-food-prices-pipeline
```

### 2. Start everything

```bash
docker compose up -d
```

Wait ~60 seconds for pip installs and DB migrations to complete.

### 3. Access the UIs

| Service | URL | Login |
|---------|-----|-------|
| Airflow | http://localhost:8080 | any username (auto-admin) |
| Grafana | http://localhost:3000 | admin / admin |

### 4. Trigger the DAGs

In Airflow UI, click the **play button ▶️** on each DAG:
- `kenya_food_prices_pipeline` (Project 1)
- `wfp_kenya_food_prices_etl` (Project 2)

### 5. View results

- **pgAdmin / psql** — connect to `localhost:5435` (Project 1) or `localhost:5434` (Project 2)
- **Grafana** — pre-provisioned dashboards at http://localhost:3000

### 6. Stop

```bash
docker compose down          # keep data
docker compose down -v       # remove data volumes too
```

---

## Repository Structure

```
kenya-food-prices-pipeline/
├── docker-compose.yml                  # Unified: 3 Postgres + Airflow + Grafana
├── .gitignore
├── README.md                           # ← You are here
│
├── Data_Engineering_Project1/          # SQL-first ETL
│   ├── docker-compose.yaml             # Standalone compose (uses .env for secrets)
│   ├── .env.example                    # Template for required environment variables
│   ├── dags/
│   │   └── kenya_food_prices_pipeline.py
│   ├── wfp_food_prices_ken.csv
│   ├── food_prices.sql                 # DDL + analytic queries
│   ├── load_raw_food_prices.py         # Standalone Python loader (env-var credentials)
│   ├── clean1.py                       # Data cleaning: fix dtypes, drop nulls & duplicates
│   ├── Null_values_check.py            # Data quality exploration (legacy)
│   ├── rows_with_nulls.csv             # Rows with missing location data
│   └── README.md
│
├── Data_Engineering_Project2/          # Python + dbt ELT
│   ├── docker-compose.yml              # Standalone compose (single-project)
│   ├── dags/
│   │   └── wfp_food_prices_dag.py
│   ├── Scripts/
│   │   ├── config.py
│   │   ├── extract.py
│   │   ├── clean.py
│   │   ├── quality_checks.py
│   │   ├── load.py
│   │   ├── pipeline.py
│   │   └── snowflake_load.py
│   ├── SQL/
│   │   └── schema.sql
│   ├── dbt/food_prices_dbt/
│   │   ├── dbt_project.yml
│   │   ├── profiles.yml
│   │   └── models/
│   ├── grafana/
│   │   ├── provisioning/
│   │   └── dashboards/
│   ├── wfp_food_prices_ken.csv
│   ├── requirements.txt
│   ├── .env.example
│   └── README.md
│
└── sample_outputs/                     # Pre-generated query results (CSV)
    ├── project1_avg_price_per_commodity_year.csv
    ├── project1_avg_price_per_category.csv
    ├── project2_fact_prices_sample.csv
    ├── project2_dim_commodity.csv
    ├── project2_dim_market.csv
    └── project2_dim_date.csv
```

---

## Technology Stack

| Layer | Technology | Version |
|-------|-----------|---------|
| Orchestration | Apache Airflow | 3.1.7 |
| Data Warehouse | PostgreSQL | 17 |
| Transformation (Project 2) | dbt Core + dbt-postgres | 1.11 |
| Data Processing | Python / pandas | 3.12 |
| Visualisation | Grafana | 11.1.0 |
| Containerisation | Docker Compose | v2 |

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Two separate pipelines** | Demonstrates both SQL-first (ETL) and Python/dbt (ELT) approaches |
| **Single Airflow instance** | Unified orchestration — mirrors production multi-tenant pattern |
| **Separate data warehouses** | Project isolation — each pipeline owns its database |
| **Star schema (Project 2)** | Optimised for analytical queries with predictable joins |
| **Incremental loading** | Avoids reprocessing; date-based watermark via `max(date)` |
| **Idempotent tasks** | `TRUNCATE` before load (P1), `CREATE TABLE IF NOT EXISTS` |
| **price_per_kg normalisation** | 14 different units in dataset → normalise to per-kg for comparability |
| **Parquet for XCom** | Efficient serialisation between Airflow tasks |
| **Docker Compose** | Single-command reproducible infrastructure |

---

## Data Quality Issues Found

| Issue | Rows Affected | Handling |
|-------|---------------|----------|
| Missing lat/lon + admin region | 47 | Forward-filled within market groups |
| Commodity naming variants (e.g. `Maize` vs `Maize (white)`) | ~500 | Documented; title-cased in cleaning |
| Hybrid `priceflag` values (`actual,aggregate`) | ~200 | Kept as-is; noted in quality report |
| Mixed units (KG, 90 KG, 50 KG, 400 G, etc.) | All | Normalised to price_per_kg in Project 2 |

---

## Sample Outputs

Pre-generated CSVs in [`sample_outputs/`](sample_outputs/) for review without running the pipeline:

- **Project 1:** `avg_price_per_commodity_year.csv`, `avg_price_per_category.csv`
- **Project 2:** `fact_prices_sample.csv`, `dim_commodity.csv`, `dim_market.csv`, `dim_date.csv`

---

## Author

Built as a Data Engineering portfolio project demonstrating end-to-end pipeline development with modern tooling.

---

## License

This project uses publicly available WFP food price data from the [Humanitarian Data Exchange (HDX)](https://data.humdata.org/dataset/wfp-food-prices-for-kenya).
