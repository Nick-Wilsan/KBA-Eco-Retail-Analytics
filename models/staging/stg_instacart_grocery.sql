WITH source AS (
    SELECT * FROM {{ source('eco_retail_bronze', 'instacart_grocery') }}
),

renamed_and_casted AS (
    SELECT
        CAST(order_id AS INTEGER) AS order_id,
        CAST(product_id AS INTEGER) AS product_id,
        CAST(add_to_cart_order AS INTEGER) AS add_to_cart_order,
        CAST(reordered AS BOOLEAN) AS is_reordered,
        CAST(_loaded_at AS TIMESTAMP) AS ingested_at,
        _source_file AS source_file_path
    FROM source
)

SELECT * FROM renamed_and_casted