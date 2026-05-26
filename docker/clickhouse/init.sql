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
