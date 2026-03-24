
--"How has the price of Maize (white) changed from 2006 to 2025?"
SELECT
    EXTRACT(YEAR FROM date)  AS price_year,
    commodity,
    pricetype,
    COUNT(*)                      AS num_observations,
    ROUND(AVG(price), 2)          AS avg_price_kes,
    ROUND(AVG(usdprice), 4)       AS avg_price_usd
FROM cleaned_food_prices
WHERE commodity LIKE 'Maize (white)'
GROUP BY EXTRACT(YEAR FROM date), commodity, pricetype
ORDER BY commodity, price_year;

--"Is the wholesale price of beans increasing faster than the retail price?"
SELECT 
    EXTRACT(YEAR FROM date) AS observation_year,
    ROUND(AVG(CASE WHEN LOWER(pricetype) = 'wholesale' THEN price END), 2) AS avg_wholesale_price,
    ROUND(AVG(CASE WHEN LOWER(pricetype) = 'retail' THEN price END), 2) AS avg_retail_price
FROM raw_food_prices
WHERE LOWER(commodity) LIKE '%beans%'
GROUP BY EXTRACT(YEAR FROM date)
ORDER BY observation_year ASC;

--"Which commodity has seen the sharpest price increase over the last 5 years?"
SELECT 
    commodity,
    ROUND(AVG(CASE WHEN EXTRACT(YEAR FROM date) = 2019 THEN price END), 2) AS price_2019,
    ROUND(AVG(CASE WHEN EXTRACT(YEAR FROM date) = 2024 THEN price END), 2) AS price_2024,
    ROUND(
        ((AVG(CASE WHEN EXTRACT(YEAR FROM date) = 2024 THEN price END) - 
          AVG(CASE WHEN EXTRACT(YEAR FROM date) = 2019 THEN price END)) 
        / AVG(CASE WHEN EXTRACT(YEAR FROM date) = 2019 THEN price END)) * 100, 
    2) AS percentage_increase
FROM raw_food_prices
WHERE EXTRACT(YEAR FROM date) IN (2019, 2024) AND commodity NOT LIKE 'Fuel%' AND commodity NOT LIKE 'Maize%' AND commodity NOT LIKE 'Bread'
GROUP BY commodity
HAVING AVG(CASE WHEN EXTRACT(YEAR FROM date) = 2019 THEN price END) > 0
ORDER BY percentage_increase DESC;