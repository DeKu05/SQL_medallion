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
LOAD DATA LOCAL INFILE 'D:/SQL_medallion/data/orders.json'
INTO TABLE bronze_orders
FIELDS TERMINATED BY '\n'
(raw_json);


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
FROM bronze_orders;


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