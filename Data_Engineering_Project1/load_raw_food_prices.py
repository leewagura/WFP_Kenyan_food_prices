import os
import pandas as pd
from sqlalchemy import create_engine

def load_csv_to_postgres():
    DB_USER = os.environ["DB_USER"]
    DB_PASSWORD = os.environ["DB_PASSWORD"]
    DB_HOST = os.environ.get("DB_HOST", "localhost")
    DB_PORT = os.environ.get("DB_PORT", "5432")
    DB_NAME = os.environ.get("DB_NAME", "kenyan_food_prices")
    TABLE_NAME = "raw_food_prices"
    CSV_FILE_PATH = "wfp_food_prices_ken.csv"

    db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(db_url)

    try:
        df = pd.read_csv(CSV_FILE_PATH, parse_dates=["date"])
        print(f"Successfully loaded {len(df)} rows into Pandas DataFrame.")
        df.to_sql(TABLE_NAME, engine, if_exists="append", index=False)
        print(f"Successfully loaded data into table '{TABLE_NAME}' in PostgreSQL.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    load_csv_to_postgres()