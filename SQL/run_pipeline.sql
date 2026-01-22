CREATE DATABASE IF NOT EXISTS medallion_db;
USE medallion_db;

-- ===============================
DROP TABLE IF EXISTS bronze_orders;

CREATE TABLE bronze_orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    raw_json JSON,
    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
SET GLOBAL local_infile = 1;
LOAD DATA LOCAL INFILE 'D:/SQL_medallion/data/fault.json'
INTO TABLE bronze_orders
FIELDS TERMINATED BY '\n'
(raw_json);

SELECT 
    'Data Quality Check' AS check_type,
    COUNT(*) AS invalid_records,
    CASE 
        WHEN COUNT(*) > 0 THEN 'FAIL - Found negative amounts'
        ELSE 'PASS - All amounts are valid'
    END AS result
FROM bronze_orders
WHERE CAST(JSON_UNQUOTE(raw_json->'$.amount') AS DECIMAL(10,2)) <= 0;

-- ===============================
DROP TABLE IF EXISTS silver_orders;

CREATE TABLE silver_orders (
    order_id INT,
    customer VARCHAR(100),
    amount DECIMAL(10,2),
    city VARCHAR(50),
    order_date DATE,
    ingestion_time TIMESTAMP
);

INSERT INTO silver_orders
SELECT
    JSON_UNQUOTE(raw_json->'$.order_id') AS order_id,
    JSON_UNQUOTE(raw_json->'$.customer') AS customer,
    CAST(JSON_UNQUOTE(raw_json->'$.amount') AS DECIMAL(10,2)) AS amount,
    JSON_UNQUOTE(raw_json->'$.city') AS city,
    CAST(JSON_UNQUOTE(raw_json->'$.order_date') AS DATE) AS order_date,
    ingestion_time
FROM bronze_orders
WHERE CAST(JSON_UNQUOTE(raw_json->'$.amount') AS DECIMAL(10,2)) > 0;


-- ===============================
DROP TABLE IF EXISTS gold_city_sales;

CREATE TABLE gold_city_sales (
    city VARCHAR(50),
    total_sales DECIMAL(12,2)
);

INSERT INTO gold_city_sales
SELECT
    city,
    SUM(amount) AS total_sales
FROM silver_orders
GROUP BY city;

-- ===============================

SELECT * FROM gold_city_sales;

-- ===============================

SELECT 
    'DATA INTEGRITY CHECK' AS check_name,
    (SELECT COUNT(*) FROM bronze_orders) AS bronze_records,
    (SELECT COUNT(*) FROM silver_orders) AS silver_records,
    (SELECT COUNT(*) FROM bronze_orders) - (SELECT COUNT(*) FROM silver_orders) AS records_filtered,
    CASE 
        WHEN (SELECT COUNT(*) FROM bronze_orders) >= (SELECT COUNT(*) FROM silver_orders)
        THEN 'PASS - Data pipeline integrity maintained'
        ELSE 'WARNING - Check for data loss'
    END AS validation_result;

-- ===============================

SELECT 
    'TOP 3 PERFORMERS' AS category,
    city,
    CONCAT('₹', FORMAT(total_sales, 2)) AS sales
FROM gold_city_sales
ORDER BY total_sales DESC
LIMIT 3;

-- ==============================

SELECT 
    'BOTTOM 3 PERFORMERS' AS category,
    city,
    CONCAT('₹', FORMAT(total_sales, 2)) AS sales
FROM gold_city_sales
ORDER BY total_sales ASC
LIMIT 3;
