{{
  config(
    materialized='view',
    schema='food_prices'
  )
}}

/*
  agg_monthly_avg_price.sql
  Monthly average price per county and commodity.
*/

SELECT
    d.year,
    d.month,
    DATE_TRUNC('month', d.date_id)::DATE  AS price_month,
    m.county,
    c.commodity,
    c.category,
    COUNT(*)                               AS observation_count,
    ROUND(AVG(f.price)::NUMERIC, 2)        AS avg_price_kes,
    ROUND(AVG(f.usd_price)::NUMERIC, 4)    AS avg_usd_price,
    ROUND(AVG(f.price_per_kg)::NUMERIC, 2) AS avg_price_per_kg,
    ROUND(MIN(f.price)::NUMERIC, 2)        AS min_price,
    ROUND(MAX(f.price)::NUMERIC, 2)        AS max_price
FROM {{ source('food_prices', 'fact_prices') }} f
JOIN {{ source('food_prices', 'dim_date') }} d
    ON f.date_id = d.date_id
JOIN {{ source('food_prices', 'dim_commodity') }} c
    ON f.commodity_id = c.commodity_id
JOIN {{ source('food_prices', 'dim_market') }} m
    ON f.market_id = m.market_id
GROUP BY d.year, d.month, DATE_TRUNC('month', d.date_id), m.county, c.commodity, c.category
ORDER BY price_month DESC, county, commodity
