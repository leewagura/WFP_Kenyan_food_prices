
-- Star Schema DDL for WFP Kenya Food Prices
-- Database: food_prices_kenya  (PostgreSQL 17)


-- 0. Create schema
CREATE SCHEMA IF NOT EXISTS food_prices;


-- DIMENSION TABLES


-- dim_date: calendar dimension
CREATE TABLE IF NOT EXISTS food_prices.dim_date (
    date_id         DATE        PRIMARY KEY,
    year            INT         NOT NULL,
    month           INT         NOT NULL,
    month_name      VARCHAR(20) NOT NULL,
    quarter         INT         NOT NULL,
    day_of_week     INT,
    is_weekend      BOOLEAN
);

-- dim_commodity: commodity / category dimension
CREATE TABLE IF NOT EXISTS food_prices.dim_commodity (
    commodity_id    INT         PRIMARY KEY,
    commodity       VARCHAR(120) NOT NULL,
    category        VARCHAR(80)  NOT NULL,
    unit            VARCHAR(20)  NOT NULL
);

-- dim_market: market / geographic dimension
CREATE TABLE IF NOT EXISTS food_prices.dim_market (
    market_id       INT         PRIMARY KEY,
    market          VARCHAR(120) NOT NULL,
    region          VARCHAR(80)  NOT NULL,
    county          VARCHAR(80)  NOT NULL,
    latitude        NUMERIC(8,4),
    longitude       NUMERIC(8,4)
);


-- STAGING TABLE  (cleaned flat data before star-schema load)

CREATE TABLE IF NOT EXISTS food_prices.stg_cleaned_prices (
    id              SERIAL      PRIMARY KEY,
    date            DATE        NOT NULL,
    region          VARCHAR(80),
    county          VARCHAR(80),
    market          VARCHAR(120),
    market_id       INT,
    latitude        NUMERIC(8,4),
    longitude       NUMERIC(8,4),
    category        VARCHAR(80),
    commodity       VARCHAR(120),
    commodity_id    INT,
    unit            VARCHAR(20),
    price_flag      VARCHAR(30),
    price_type      VARCHAR(20),
    currency        VARCHAR(10),
    price           NUMERIC(12,4),
    usd_price       NUMERIC(12,4),
    year            INT,
    month           INT,
    price_per_kg    NUMERIC(12,4),
    usd_price_per_kg NUMERIC(12,4),
    loaded_at       TIMESTAMP   DEFAULT NOW()
);


-- FACT TABLE

CREATE TABLE IF NOT EXISTS food_prices.fact_prices (
    fact_id         SERIAL      PRIMARY KEY,
    date_id         DATE        NOT NULL REFERENCES food_prices.dim_date(date_id),
    commodity_id    INT         NOT NULL REFERENCES food_prices.dim_commodity(commodity_id),
    market_id       INT         NOT NULL REFERENCES food_prices.dim_market(market_id),
    price_type      VARCHAR(20),
    price_flag      VARCHAR(30),
    currency        VARCHAR(10),
    price           NUMERIC(12,4),
    usd_price       NUMERIC(12,4),
    price_per_kg    NUMERIC(12,4),
    usd_price_per_kg NUMERIC(12,4),
    loaded_at       TIMESTAMP   DEFAULT NOW()
);


-- INDEXES for common query patterns

CREATE INDEX IF NOT EXISTS idx_fact_date      ON food_prices.fact_prices (date_id);
CREATE INDEX IF NOT EXISTS idx_fact_commodity  ON food_prices.fact_prices (commodity_id);
CREATE INDEX IF NOT EXISTS idx_fact_market     ON food_prices.fact_prices (market_id);
CREATE INDEX IF NOT EXISTS idx_stg_date       ON food_prices.stg_cleaned_prices (date);
