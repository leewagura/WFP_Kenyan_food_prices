CREATE TABLE IF NOT EXISTS raw_food_prices (
	date DATE,
	admin1 VARCHAR(50),
	admin2 VARCHAR(50),
	market VARCHAR(100),
	market_id INTEGER,
	latitude NUMERIC(8, 4),
	longitude NUMERIC(8, 4),
	category VARCHAR(50),
	commodity VARCHAR(100),
	commodity_id INTEGER,
	unit VARCHAR(50),
	priceflag VARCHAR(50),
	pricetype VARCHAR(50),
	currency VARCHAR(10),
	price NUMERIC(8, 2),
	usdprice NUMERIC(8, 2)
);

--average wholesale price of "Maize (white)" per year to observe inflation trends over time.
SELECT 
    EXTRACT(YEAR FROM date) as price_year,
    commodity,
    ROUND(AVG(price), 2) as avg_yearly_price
FROM raw_food_prices
WHERE commodity = 'Maize (white)' AND pricetype = 'Wholesale'
GROUP BY EXTRACT(YEAR FROM date), commodity
ORDER BY price_year ASC;

--commodities that are tracked in more than 10 distinct markets.

SELECT 
    commodity, 
    COUNT(DISTINCT market) as number_of_markets
FROM raw_food_prices
GROUP BY commodity
HAVING COUNT(DISTINCT market) > 10
ORDER BY number_of_markets DESC;

--markets offering the highest variety of unique commodities and categories
SELECT 
    admin2 AS county,
    market,
    COUNT(DISTINCT category) AS total_categories,
    COUNT(DISTINCT commodity) AS unique_commodities_sold
FROM raw_food_prices
GROUP BY admin2, market
ORDER BY unique_commodities_sold DESC
LIMIT 10;

