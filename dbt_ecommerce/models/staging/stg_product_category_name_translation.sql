SELECT
    product_category_name,
    product_category_name_english
FROM {{ source('ecommerce', 'product_category_name_translation') }}
