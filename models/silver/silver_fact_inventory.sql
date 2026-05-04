{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_fact_inv_product ON {{ this }} (product_id)",
        "CREATE INDEX IF NOT EXISTS idx_fact_inv_store ON {{ this }} (store_id)"
    ]
) }}

WITH inventory_data AS (
    SELECT
        id AS inventory_record_id,
        CAST(item_id AS VARCHAR) AS product_id,
        CAST(store_id AS VARCHAR) AS store_id,
        CAST(date_std AS DATE) AS date_id,
        CAST(sales_qty AS INTEGER) AS stock_quantity,
        CAST(sell_price AS DOUBLE) AS current_price
    FROM {{ ref('stg_m5_forcasting') }}
    WHERE id IS NOT NULL
)

SELECT * FROM inventory_data
ORDER BY date_id