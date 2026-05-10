WITH sales_data AS (
    SELECT
        CAST(order_item_id AS VARCHAR) AS order_item_id,
        CAST(order_id AS VARCHAR) AS order_id,
        CAST(order_date + INTERVAL 2 YEAR + INTERVAL 4 MONTH AS DATE) AS order_date_id,
        CAST(customer_id AS VARCHAR) AS customer_id,
        CAST(store_id AS VARCHAR) AS store_id,
        CAST(product_id AS VARCHAR) AS product_id,
        CAST(qty AS INTEGER) AS quantity,
        CAST(unit_price AS DOUBLE) AS unit_price,
        CAST(discount_applied AS DOUBLE) AS discount_amount,
        CAST(total_order_payment AS DOUBLE) AS total_payment,
        shipment_status
    FROM "warehouse"."warehouse"."stg_retail_data"
    WHERE order_item_id IS NOT NULL
)

SELECT * FROM sales_data
ORDER BY order_date_id