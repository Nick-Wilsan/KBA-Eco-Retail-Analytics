import duckdb

try:
    print('Fixing Category and Store names in Gold Waste Summary...')
    conn = duckdb.connect('data/warehouse.duckdb')
    
    conn.execute('''
    CREATE OR REPLACE TABLE gold.gold_mart_food_waste_summary AS
    WITH inventory AS (
        SELECT 
            date_id,
            product_id,
            store_id,
            SUM(stock_quantity) AS total_stock,
            AVG(current_price) AS avg_price
        FROM silver.silver_fact_inventory
        GROUP BY 1, 2, 3
    ),
    daily_sales AS (
        SELECT 
            order_date_id AS date_id,
            SUM(quantity) AS total_sold_per_day
        FROM silver.silver_fact_sales
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
        
        -- FORMAT STORE NAME: Dari 'CA_1' menjadi 'California Store 1'
        i.store_id,
        CASE 
            WHEN i.store_id LIKE 'CA_%' THEN 'California Store ' || SUBSTRING(i.store_id, 4, 1)
            WHEN i.store_id LIKE 'TX_%' THEN 'Texas Store ' || SUBSTRING(i.store_id, 4, 1)
            WHEN i.store_id LIKE 'WI_%' THEN 'Wisconsin Store ' || SUBSTRING(i.store_id, 4, 1)
            ELSE i.store_id 
        END AS store_name,
        
        i.product_id,
        
        -- FORMAT CATEGORY: Ekstrak dari M5 product_id (contoh: FOODS_3_370 -> Foods)
        CASE 
            WHEN i.product_id LIKE 'FOODS_%' THEN 'Food & Beverage'
            WHEN i.product_id LIKE 'HOUSEHOLD_%' THEN 'Household & Cleaning'
            WHEN i.product_id LIKE 'HOBBIES_%' THEN 'Hobbies & Toys'
            ELSE 'Others' 
        END AS category_name,
        
        i.total_stock,
        
        -- Distribusi Penjualan Harian
        COALESCE(CAST(ROUND(ds.total_sold_per_day * 1.0 / iwc.inv_rows_per_day) AS BIGINT), 0) AS total_sold,
        
        GREATEST(i.total_stock - COALESCE(CAST(ROUND(ds.total_sold_per_day * 1.0 / iwc.inv_rows_per_day) AS BIGINT), 0), 0) AS unsold_qty,
        
        GREATEST(i.total_stock - COALESCE(CAST(ROUND(ds.total_sold_per_day * 1.0 / iwc.inv_rows_per_day) AS BIGINT), 0), 0) * i.avg_price AS potential_waste_value,
        
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
    ''')
    print('Gold Waste Summary successfully rebuilt with beautiful Store and Category Names!')

    # Test
    print('\n--- Category Check ---')
    print(conn.execute('SELECT category_name, SUM(unsold_qty) FROM gold.gold_mart_food_waste_summary GROUP BY category_name').fetchdf())
    
    print('\n--- Store Check ---')
    print(conn.execute('SELECT store_name, SUM(unsold_qty) FROM gold.gold_mart_food_waste_summary GROUP BY store_name LIMIT 5').fetchdf())

    conn.close()
except Exception as e:
    print(f'Error: {e}')
