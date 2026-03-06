{{
  config(
    materialized='view',
    schema='food_prices'
  )
}}

/*
  agg_price_trends.sql
  Long-term price trends: year-over-year change per commodity.
*/

WITH yearly AS (
    SELECT
        d.year,
        c.commodity,
        c.category,
        ROUND(AVG(f.usd_price)::NUMERIC, 4)        AS avg_usd_price,
        ROUND(AVG(f.price_per_kg)::NUMERIC, 4)      AS avg_price_per_kg_kes,
        COUNT(*)                                     AS observations
    FROM {{ source('food_prices', 'fact_prices') }} f
    JOIN {{ source('food_prices', 'dim_date') }} d
        ON f.date_id = d.date_id
    JOIN {{ source('food_prices', 'dim_commodity') }} c
        ON f.commodity_id = c.commodity_id
    GROUP BY d.year, c.commodity, c.category
)

SELECT
    y.*,
    LAG(y.avg_usd_price) OVER (PARTITION BY y.commodity ORDER BY y.year)
        AS prev_year_usd_price,
    ROUND(
        (y.avg_usd_price - LAG(y.avg_usd_price) OVER (PARTITION BY y.commodity ORDER BY y.year))
        / NULLIF(LAG(y.avg_usd_price) OVER (PARTITION BY y.commodity ORDER BY y.year), 0) * 100,
        2
    ) AS yoy_change_pct
FROM yearly y
ORDER BY y.commodity, y.year
