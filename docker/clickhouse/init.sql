CREATE DATABASE IF NOT EXISTS ecommerce;

USE ecommerce;

-- 1. Customers table
CREATE TABLE IF NOT EXISTS customers (
    customer_id String,
    customer_unique_id String,
    customer_zip_code_prefix String,
    customer_city String,
    customer_state String
) ENGINE = MergeTree()
PRIMARY KEY customer_id
ORDER BY customer_id;

-- 2. Geolocation table
CREATE TABLE IF NOT EXISTS geolocation (
    geolocation_zip_code_prefix String,
    geolocation_lat Float64,
    geolocation_lng Float64,
    geolocation_city String,
    geolocation_state String
) ENGINE = MergeTree()
ORDER BY (geolocation_zip_code_prefix, geolocation_city);

-- 3. Products table (utilizes Nullable for fields that can contain nulls in CSV)
CREATE TABLE IF NOT EXISTS products (
    product_id String,
    product_category_name Nullable(String),
    product_name_lenght Nullable(Int32),
    product_description_lenght Nullable(Int32),
    product_photos_qty Nullable(Int32),
    product_weight_g Nullable(Float64),
    product_length_cm Nullable(Float64),
    product_height_cm Nullable(Float64),
    product_width_cm Nullable(Float64)
) ENGINE = MergeTree()
PRIMARY KEY product_id
ORDER BY product_id;

-- 4. Sellers table
CREATE TABLE IF NOT EXISTS sellers (
    seller_id String,
    seller_zip_code_prefix String,
    seller_city String,
    seller_state String
) ENGINE = MergeTree()
PRIMARY KEY seller_id
ORDER BY seller_id;

-- 5. Product Category Name Translation table
CREATE TABLE IF NOT EXISTS product_category_name_translation (
    product_category_name String,
    product_category_name_english String
) ENGINE = MergeTree()
PRIMARY KEY product_category_name
ORDER BY product_category_name;

-- =========================================================================
-- STREAMING PIPELINE TABLES (Orders, Items, Payments, Reviews)
-- =========================================================================

-- 6. Target MergeTree Tables
CREATE TABLE IF NOT EXISTS orders (
    order_id String,
    customer_id String,
    order_status String,
    order_purchase_timestamp DateTime,
    order_approved_at Nullable(DateTime),
    order_delivered_carrier_date Nullable(DateTime),
    order_delivered_customer_date Nullable(DateTime),
    order_estimated_delivery_date DateTime
) ENGINE = MergeTree()
PRIMARY KEY order_id
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS order_items (
    order_id String,
    order_item_id Int32,
    product_id String,
    seller_id String,
    shipping_limit_date DateTime,
    price Float64,
    freight_value Float64
) ENGINE = MergeTree()
ORDER BY (order_id, order_item_id);

CREATE TABLE IF NOT EXISTS order_payments (
    order_id String,
    payment_sequential Int32,
    payment_type String,
    payment_installments Int32,
    payment_value Float64
) ENGINE = MergeTree()
ORDER BY (order_id, payment_sequential);

CREATE TABLE IF NOT EXISTS order_reviews (
    review_id String,
    order_id String,
    review_score Int32,
    review_comment_title Nullable(String),
    review_comment_message Nullable(String),
    review_creation_date DateTime,
    review_answer_timestamp DateTime
) ENGINE = MergeTree()
PRIMARY KEY review_id
ORDER BY review_id;

-- 7. Kafka Engine Queue Tables (Consume JSON payloads from Kafka)
CREATE TABLE IF NOT EXISTS orders_queue (
    order_id String,
    customer_id String,
    order_status String,
    order_purchase_timestamp String,
    order_approved_at Nullable(String),
    order_delivered_carrier_date Nullable(String),
    order_delivered_customer_date Nullable(String),
    order_estimated_delivery_date String
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka1:9092,kafka2:9092,kafka3:9092',
         kafka_topic_list = 'orders',
         kafka_group_name = 'clickhouse_orders_group',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS order_items_queue (
    order_id String,
    order_item_id Int32,
    product_id String,
    seller_id String,
    shipping_limit_date String,
    price Float64,
    freight_value Float64
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka1:9092,kafka2:9092,kafka3:9092',
         kafka_topic_list = 'order_items',
         kafka_group_name = 'clickhouse_order_items_group',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS order_payments_queue (
    order_id String,
    payment_sequential Int32,
    payment_type String,
    payment_installments Int32,
    payment_value Float64
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka1:9092,kafka2:9092,kafka3:9092',
         kafka_topic_list = 'order_payments',
         kafka_group_name = 'clickhouse_order_payments_group',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS order_reviews_queue (
    review_id String,
    order_id String,
    review_score Int32,
    review_comment_title Nullable(String),
    review_comment_message Nullable(String),
    review_creation_date String,
    review_answer_timestamp String
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka1:9092,kafka2:9092,kafka3:9092',
         kafka_topic_list = 'order_reviews',
         kafka_group_name = 'clickhouse_order_reviews_group',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1;

-- 8. Materialized Views (Transform raw formats and insert into Target Tables)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_orders TO orders AS
SELECT
    order_id,
    customer_id,
    order_status,
    parseDateTimeBestEffortOrNull(order_purchase_timestamp) AS order_purchase_timestamp,
    parseDateTimeBestEffortOrNull(order_approved_at) AS order_approved_at,
    parseDateTimeBestEffortOrNull(order_delivered_carrier_date) AS order_delivered_carrier_date,
    parseDateTimeBestEffortOrNull(order_delivered_customer_date) AS order_delivered_customer_date,
    parseDateTimeBestEffortOrNull(order_estimated_delivery_date) AS order_estimated_delivery_date
FROM orders_queue;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_order_items TO order_items AS
SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    parseDateTimeBestEffortOrNull(shipping_limit_date) AS shipping_limit_date,
    price,
    freight_value
FROM order_items_queue;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_order_payments TO order_payments AS
SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
FROM order_payments_queue;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_order_reviews TO order_reviews AS
SELECT
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    parseDateTimeBestEffortOrNull(review_creation_date) AS review_creation_date,
    parseDateTimeBestEffortOrNull(review_answer_timestamp) AS review_answer_timestamp
FROM order_reviews_queue;

