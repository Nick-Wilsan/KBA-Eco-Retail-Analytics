WITH daily_sales AS (
    SELECT 
        order_date_id AS date_id,
        product_id,
        store_id,
        SUM(quantity) AS daily_sales_qty,
        SUM(total_payment) AS daily_revenue,
        COUNT(DISTINCT order_id) AS total_orders
    FROM "warehouse"."silver"."silver_fact_sales"
    GROUP BY 1, 2, 3
)

SELECT 
    d.date_id,
    d.year,
    d.month,
    d.day_name,
    ds.store_id,
    ds.product_id,
    p.category_name,
    ds.daily_sales_qty,
    ds.daily_revenue,
    ds.total_orders
FROM daily_sales ds
JOIN "warehouse"."silver"."silver_dim_date" d 
    ON ds.date_id = d.date_id
LEFT JOIN "warehouse"."silver"."silver_dim_product" p
    ON ds.product_id = p.product_id
ORDER BY ds.date_id