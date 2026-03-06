{{
  config(
    materialized='view',
    schema='food_prices'
  )
}}

/*
  agg_commodity_summary.sql
  Overall summary statistics per commodity – useful for bar charts.
*/

SELECT
    c.commodity,
    c.category,
    c.unit,
    COUNT(*)                                    AS total_observations,
    ROUND(AVG(f.price)::NUMERIC, 2)             AS avg_price_kes,
    ROUND(AVG(f.usd_price)::NUMERIC, 4)         AS avg_usd_price,
    ROUND(STDDEV(f.usd_price)::NUMERIC, 4)      AS stddev_usd_price,
    ROUND(MIN(f.usd_price)::NUMERIC, 4)         AS min_usd_price,
    ROUND(MAX(f.usd_price)::NUMERIC, 4)         AS max_usd_price,
    MIN(f.date_id)                               AS first_observed,
    MAX(f.date_id)                               AS last_observed
FROM {{ source('food_prices', 'fact_prices') }} f
JOIN {{ source('food_prices', 'dim_commodity') }} c
    ON f.commodity_id = c.commodity_id
GROUP BY c.commodity, c.category, c.unit
ORDER BY avg_usd_price DESC
