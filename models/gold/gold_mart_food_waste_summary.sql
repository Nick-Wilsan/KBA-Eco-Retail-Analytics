WITH inventory AS (
    SELECT 
        date_id,
        product_id,
        store_id,
        SUM(stock_quantity) AS total_stock,
        AVG(current_price) AS avg_price
    FROM {{ ref('silver_fact_inventory') }}
    GROUP BY 1, 2, 3
),
daily_sales AS (
    SELECT 
        order_date_id AS date_id,
        SUM(quantity) AS total_sold_per_day
    FROM {{ ref('silver_fact_sales') }}
    GROUP BY 1
),
inventory_with_counts AS (
    SELECT 
        date_id,
        COUNT(*) AS inv_rows_per_day
    FROM inventory
    GROUP BY 1
)

SELECT 
    i.date_id,
    i.store_id,
    st.store_name,
    i.product_id,
    
    CASE 
        WHEN i.product_id LIKE 'FOODS%' THEN 'Food & Beverage'
        WHEN i.product_id LIKE 'HOUSEHOLD%' THEN 'Household & Cleaning'
        WHEN i.product_id LIKE 'HOBBIES%' THEN 'Hobbies & Toys'
        ELSE COALESCE(p.category_name, 'Other')
    END AS category_name,
    
    i.total_stock,
    
    COALESCE(CAST(ROUND(ds.total_sold_per_day * 1.0 / iwc.inv_rows_per_day) AS BIGINT), 0) AS total_sold,
    
    GREATEST(i.total_stock - COALESCE(CAST(ROUND(ds.total_sold_per_day * 1.0 / iwc.inv_rows_per_day) AS BIGINT), 0), 0) AS unsold_qty,
    
    -- Menghitung nilai kerugian (Waste Value) dengan fallback price jika avg_price kosong, dikalikan 15000 (Konversi ke Rupiah/IDR)
    GREATEST(i.total_stock - COALESCE(CAST(ROUND(ds.total_sold_per_day * 1.0 / iwc.inv_rows_per_day) AS BIGINT), 0), 0) * 
    COALESCE(
        i.avg_price, 
        p.default_price, 
        CASE 
            WHEN i.product_id LIKE 'FOODS%' THEN 15.0
            WHEN i.product_id LIKE 'HOUSEHOLD%' THEN 25.0
            WHEN i.product_id LIKE 'HOBBIES%' THEN 40.0
            ELSE 10.0 
        END
    ) * 15000 AS potential_waste_value,
    
    CASE 
        WHEN i.total_stock = 0 THEN 0.0
        ELSE ROUND((GREATEST(i.total_stock - COALESCE(CAST(ROUND(ds.total_sold_per_day * 1.0 / iwc.inv_rows_per_day) AS BIGINT), 0), 0) * 100.0) / i.total_stock, 2)
    END AS waste_rate_pct,
    
    CASE 
        WHEN i.total_stock = 0 THEN 'No Stock'
        WHEN (GREATEST(i.total_stock - COALESCE(CAST(ROUND(ds.total_sold_per_day * 1.0 / iwc.inv_rows_per_day) AS BIGINT), 0), 0) * 100.0) / i.total_stock > 20 THEN 'High Waste Risk'
        WHEN (GREATEST(i.total_stock - COALESCE(CAST(ROUND(ds.total_sold_per_day * 1.0 / iwc.inv_rows_per_day) AS BIGINT), 0), 0) * 100.0) / i.total_stock > 5 THEN 'Moderate Risk'
        ELSE 'Healthy'
    END AS waste_rate_status

FROM inventory i
LEFT JOIN daily_sales ds ON i.date_id = ds.date_id
LEFT JOIN inventory_with_counts iwc ON i.date_id = iwc.date_id
LEFT JOIN {{ ref('silver_dim_product') }} p
    ON i.product_id = p.product_id
LEFT JOIN {{ ref('silver_dim_store') }} st
    ON i.store_id = st.store_id
