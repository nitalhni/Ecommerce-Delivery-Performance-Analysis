CREATE DATABASE olist_db;
USE olist_db;

# IMPORT DATASET

#1. Geolocation
CREATE TABLE olist_geolocation (
    geolocation_zip_code_prefix INT,
    geolocation_lat DECIMAL(10,8),
    geolocation_lng DECIMAL(11,8),
    geolocation_city VARCHAR(100),
    geolocation_state CHAR(2)
    );

LOAD DATA LOCAL INFILE 'E:/Python/Project Olist/olist_geolocation_dataset.csv'
INTO TABLE olist_geolocation
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT COUNT(*) FROM olist_geolocation;
SELECT * FROM olist_geolocation
LIMIT 10;

#2. Customers
CREATE TABLE IF NOT EXISTS olist_customers (
    customers_id VARCHAR(100),
    customers_unique_id VARCHAR(100),
    customer_zip_code_prefix INT,
    customer_city VARCHAR(100),
    customer_state CHAR(2)
);

LOAD DATA LOCAL INFILE 'E:/Python/Project Olist/olist_customers_dataset.csv'
INTO TABLE olist_customers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT COUNT(*) FROM olist_customers;
SELECT * FROM olist_customers
LIMIT 10;

#3. Orders
CREATE TABLE IF NOT EXISTS olist_orders (
    order_id VARCHAR(100),
    customers_id VARCHAR(100),
    order_status VARCHAR(50),
    order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATE
); 

LOAD DATA LOCAL INFILE 'E:/Python/Project Olist/olist_orders_dataset.csv'
INTO TABLE olist_orders
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT COUNT(*) FROM olist_orders;
SELECT * FROM olist_orders
LIMIT 10;

#4. Order Items
CREATE TABLE IF NOT EXISTS olist_order_items (
    order_id VARCHAR(100),
    order_item_id INT,
    product_id VARCHAR(100),
    seller_id VARCHAR(100),
    shipping_limit_date DATETIME,
    price DECIMAL(10,2),
    freight_value DECIMAL(10,2)
);

LOAD DATA LOCAL INFILE 'E:/Python/Project Olist/olist_order_items_dataset.csv'
INTO TABLE olist_order_items
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT COUNT(*) FROM olist_order_items;
SELECT * FROM olist_order_items
LIMIT 10;

#5. Seller

## ==================================================

# MASTER TABLE
CREATE TABLE master_orders AS
SELECT
o.order_id,
o.customers_id,
o.order_status,
o.order_purchase_timestamp,
o.order_delivered_customer_date,
o.order_estimated_delivery_date,
c.customer_city,
c.customer_state,
i.order_item_id,
i.seller_id,
i.shipping_limit_date,
i.price,
i.freight_value,
s.seller_city,
s.seller_state
FROM olist_orders o
JOIN olist_customers c
ON o.customers_id = c.customers_id
JOIN olist_order_items i
ON o.order_id = i.order_id
JOIN olist_sellers_dataset s
ON i.seller_id = s.seller_id;

SELECT * FROM master_orders;

# Deteksi NULL
SELECT 
    COUNT(*) AS null_delivered_date
FROM master_orders
WHERE order_delivered_customer_date IS NULL;


# 1. Jumlah order per status (delivered, canceled, dll).
SELECT order_status, COUNT(order_id) AS total_order
FROM master_orders
GROUP BY order_status
ORDER BY total_order;

SELECT order_id, COUNT(*) AS item_count
FROM olist_order_items
GROUP BY order_id;

# 2. Rata-rata waktu pengiriman (order_delivered_customer_date - order_purchase_timestamp).
SELECT
AVG(DATEDIFF(order_delivered_customer_date, order_purchase_timestamp)) AS avg_delivery_days
FROM master_orders
WHERE order_status = 'delivered';

# 3. Rata-rata keterlambatan (order_delivered_customer_date - order_estimated_delivery_date).
SELECT
AVG(DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date)) AS avg_delays
FROM master_orders
WHERE order_status = 'delivered';

SELECT 
    AVG(DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date)) AS avg_delay_days
FROM master_orders
WHERE order_status = 'delivered';
