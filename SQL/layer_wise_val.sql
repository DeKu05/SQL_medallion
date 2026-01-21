-- these are teh commands to validate each layer of the medallion architecture i ahve used them in the screen shots attached.

SHOW GLOBAL VARIABLES LIKE 'local_infile';

--=======================================================

SELECT COUNT(*) as total_records FROM bronze_orders;
SELECT * FROM bronze_orders LIMIT 5;

--=======================================================

SELECT COUNT(*) as total_records FROM silver_orders;
SELECT * FROM silver_orders LIMIT 10;

--=======================================================

SELECT * FROM gold_city_sales ORDER BY total_sales DESC;

--=======================================================

SELECT 'Bronze Layer' as Layer, COUNT(*) as Records FROM bronze_orders
UNION ALL
SELECT 'Silver Layer', COUNT(*) FROM silver_orders
UNION ALL
SELECT 'Gold Layer', COUNT(*) FROM gold_city_sales;
