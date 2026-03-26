from airflow import DAG  # type: ignore[import-untyped]
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator  # type: ignore[import-untyped]
from airflow.operators.python import PythonOperator  # type: ignore[import-untyped]
from datetime import datetime, timedelta

def load_csv_to_staging():
    """Load the WFP Kenya food-prices CSV into the raw_food_prices staging table."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore[import-untyped]
    import csv
    import os
    import time
    from datetime import datetime

    hook = PostgresHook(postgres_conn_id="my_postgres_db")
    conn = hook.get_conn()
    cur = conn.cursor()

    # Truncate so the task is idempotent on re-runs
    cur.execute("TRUNCATE TABLE raw_food_prices;")

    # On host startup, Docker bind mounts can take a short time to become visible.
    csv_candidates = [
        os.environ.get("FOOD_PRICES_CSV_PATH"),
        "/opt/airflow/data/wfp_food_prices_ken.csv",
        "/opt/airflow/wfp_food_prices_ken.csv",
        "wfp_food_prices_ken.csv",
    ]

    csv_path = None
    for _ in range(24):  # wait up to 2 minutes in 5-second intervals
        for candidate in csv_candidates:
            if candidate and os.path.exists(candidate):
                csv_path = candidate
                break
        if csv_path:
            break
        time.sleep(5)

    if not csv_path:
        raise FileNotFoundError(
            "Could not find wfp_food_prices_ken.csv in any expected location: "
            + ", ".join([p for p in csv_candidates if p])
        )

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for row in reader:
            # Replace empty strings with None for nullable columns
            row = [None if v == "" else v for v in row]
            # Convert date from DD/MM/YYYY to YYYY-MM-DD format
            if row[0]:  # if date exists (not None)
                try:
                    row[0] = datetime.strptime(row[0], "%d/%m/%Y").strftime("%Y-%m-%d")
                except ValueError:
                    print(f"Warning: Could not parse date '{row[0]}', setting to None")
                    row[0] = None
            cur.execute(
                """
                INSERT INTO raw_food_prices
                    (date, admin1, admin2, market, market_id, latitude, longitude,
                     category, commodity, commodity_id, unit, priceflag, pricetype,
                     currency, price, usdprice)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                row,
            )

    conn.commit()
    cur.close()
    conn.close()
    print("CSV loaded into raw_food_prices staging table.")


with DAG(
    dag_id="kenya_food_prices_pipeline",
    start_date=datetime(2026, 2, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    max_active_runs=1,
    tags=["kenya", "food_prices", "postgres"],
) as dag:

    
    # Task 1 – Create the staging table
   
    create_staging_table = SQLExecuteQueryOperator(
        task_id="create_staging_table",
        conn_id="my_postgres_db",
        sql="""
        CREATE TABLE IF NOT EXISTS raw_food_prices (
            date        DATE,
            admin1      VARCHAR(50),
            admin2      VARCHAR(50),
            market      VARCHAR(100),
            market_id   INTEGER,
            latitude    NUMERIC(8,4),
            longitude   NUMERIC(8,4),
            category    VARCHAR(50),
            commodity   VARCHAR(100),
            commodity_id INTEGER,
            unit        VARCHAR(50),
            priceflag   VARCHAR(50),
            pricetype   VARCHAR(50),
            currency    VARCHAR(10),
            price       NUMERIC(8,2),
            usdprice    NUMERIC(8,2)
        );
        """,
    )

    
    # Task 2 – Load CSV into the staging table
    
    load_csv = PythonOperator(
        task_id="load_csv_to_staging",
        python_callable=load_csv_to_staging,
    )

  
    # Task 3 – Clean data: drop nulls and duplicates
    
    clean_data = SQLExecuteQueryOperator(
        task_id="clean_data",
        conn_id="my_postgres_db",
        sql="""
        DROP TABLE IF EXISTS cleaned_food_prices;

        CREATE TABLE cleaned_food_prices AS
        SELECT DISTINCT
            date,
            admin1 AS region,
            admin2 AS county,
            market,
            market_id,
            latitude,
            longitude,
            category,
            commodity,
            commodity_id,
            unit,
            priceflag,
            pricetype,
            currency,
            price,
            usdprice
        FROM raw_food_prices
        WHERE date       IS NOT NULL
          AND admin1     IS NOT NULL
          AND admin2     IS NOT NULL
          AND market     IS NOT NULL
          AND market_id  IS NOT NULL
          AND latitude   IS NOT NULL
          AND longitude  IS NOT NULL
          AND category   IS NOT NULL
          AND commodity  IS NOT NULL
          AND commodity_id IS NOT NULL
          AND unit       IS NOT NULL
          AND priceflag  IS NOT NULL
          AND pricetype  IS NOT NULL
          AND currency   IS NOT NULL
          AND price      IS NOT NULL
          AND usdprice   IS NOT NULL;
        """,
    )


    # Task 4 – Aggregate: average price per commodity per year
 
    aggregate_prices = SQLExecuteQueryOperator(
        task_id="aggregate_avg_prices",
        conn_id="my_postgres_db",
        sql="""
        DROP TABLE IF EXISTS avg_price_per_commodity_year;

        CREATE TABLE avg_price_per_commodity_year AS
        SELECT
            EXTRACT(YEAR FROM date)::INT  AS price_year,
            commodity,
            pricetype,
            COUNT(*)                      AS num_observations,
            ROUND(AVG(price), 2)          AS avg_price_kes,
            ROUND(AVG(usdprice), 4)       AS avg_price_usd
        FROM cleaned_food_prices
        GROUP BY EXTRACT(YEAR FROM date), commodity, pricetype
        ORDER BY commodity, price_year;
        """,
    )

    # Task 5 – Aggregate: average price per category

    aggregate_by_category = SQLExecuteQueryOperator(
        task_id="aggregate_avg_price_by_category",
        conn_id="my_postgres_db",
        sql="""
        DROP TABLE IF EXISTS avg_price_per_category;

        CREATE TABLE avg_price_per_category AS
        SELECT
            category,
            COUNT(DISTINCT commodity)     AS num_commodities,
            COUNT(*)                      AS num_observations,
            ROUND(AVG(price), 2)          AS avg_price_kes,
            ROUND(AVG(usdprice), 4)       AS avg_price_usd,
            ROUND(MIN(price), 2)          AS min_price_kes,
            ROUND(MAX(price), 2)          AS max_price_kes
        FROM cleaned_food_prices
        GROUP BY category
        ORDER BY avg_price_kes DESC;
        """,
    )


    # Pipeline order

    create_staging_table >> load_csv >> clean_data >> [aggregate_prices, aggregate_by_category]