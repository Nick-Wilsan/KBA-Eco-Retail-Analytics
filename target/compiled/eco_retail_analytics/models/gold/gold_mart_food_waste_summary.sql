WITH inventory AS (
    SELECT 
        date_id,
        product_id,
        store_id,
        SUM(stock_quantity) AS total_stock,
        AVG(current_price) AS avg_price
    FROM "warehouse"."silver"."silver_fact_inventory"
    GROUP BY 1, 2, 3
),
sales AS (
    SELECT 
        order_date_id AS date_id,
        product_id,
        store_id,
        SUM(quantity) AS total_sold
    FROM "warehouse"."silver"."silver_fact_sales"
    GROUP BY 1, 2, 3
)

SELECT 
    i.date_id,
    i.store_id,
    i.product_id,
    p.category_name,
    i.total_stock,
    COALESCE(s.total_sold, 0) AS total_sold,
    GREATEST(i.total_stock - COALESCE(s.total_sold, 0), 0) AS unsold_qty,
    
    -- Menghitung nilai kerugian (Waste Value)
    GREATEST(i.total_stock - COALESCE(s.total_sold, 0), 0) * i.avg_price AS potential_waste_value,
    
    CASE 
        WHEN i.total_stock = 0 THEN 0.0
        ELSE ROUND((GREATEST(i.total_stock - COALESCE(s.total_sold, 0), 0) * 100.0) / i.total_stock, 2)
    END AS waste_rate_pct,
    
    CASE 
        WHEN i.total_stock = 0 THEN 'No Stock'
        WHEN (GREATEST(i.total_stock - COALESCE(s.total_sold, 0), 0) * 100.0) / i.total_stock > 20 THEN 'High Waste Risk'
        WHEN (GREATEST(i.total_stock - COALESCE(s.total_sold, 0), 0) * 100.0) / i.total_stock > 5 THEN 'Moderate Risk'
        ELSE 'Healthy'
    END AS waste_rate_status

FROM inventory i
LEFT JOIN sales s 
    ON i.date_id = s.date_id 
    AND i.product_id = s.product_id 
    AND i.store_id = s.store_id
LEFT JOIN "warehouse"."silver"."silver_dim_product" p
    ON i.product_id = p.product_id