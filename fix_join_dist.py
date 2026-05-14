import duckdb

try:
    print('Applying Final Fix: Alternative Mapping...')
    conn = duckdb.connect('data/warehouse.duckdb')
    
    # Pendekatan yang lebih ringan dan elegan untuk mengatasi join yang gagal
    # Kita buat aggregate sales per date saja, lalu mendistribusikannya ke baris inventory.
    # Ingat, kita hanya melakukan ini karena kita menyatukan 2 dataset Kaggle yang ID-nya tidak cocok.
    
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
            SUM(quantity) AS total_sold_per_day,
            COUNT(DISTINCT product_id) AS distinct_products_sold
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
        i.store_id,
        i.product_id,
        p.category_name,
        i.total_stock,
        
        -- Distribusi Penjualan Harian secara rata ke setiap item yang ada stoknya
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
    LEFT JOIN silver.silver_dim_product p ON i.product_id = p.product_id
    ''')
    print('Gold Waste Summary Rebuilt with Distributed Sales Mapping.')

    # Test the result to ensure it's not just a flat 99% line anymore
    print('\nTesting Waste Rate Variance:')
    q = '''
        SELECT date_id, SUM(unsold_qty) * 100.0 / SUM(total_stock) AS avg_waste_rate
        FROM gold.gold_mart_food_waste_summary
        GROUP BY date_id
        HAVING SUM(total_sold) > 0
        ORDER BY date_id
        LIMIT 5
    '''
    print(conn.execute(q).fetchdf())

    conn.close()
except Exception as e:
    print(f'Error: {e}')
