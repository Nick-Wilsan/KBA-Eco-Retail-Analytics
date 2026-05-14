import duckdb

try:
    print('Applying Time-Shifting to Silver Inventory & Rebuilding Gold Waste Summary...')
    conn = duckdb.connect('data/warehouse.duckdb')
    
    # 1. Transform Silver Inventory: Geser tanggal (date_std) dari 2011 ke 2020 (Interval + 9 Tahun)
    conn.execute('''
    CREATE OR REPLACE TABLE silver.silver_fact_inventory AS
    SELECT
        id AS inventory_record_id,
        CAST(item_id AS VARCHAR) AS product_id,
        CAST(store_id AS VARCHAR) AS store_id,
        CAST(date_std + INTERVAL 9 YEAR AS DATE) AS date_id,
        CAST(sales_qty AS INTEGER) AS stock_quantity,
        CAST(sell_price AS DOUBLE) AS current_price
    FROM bronze.m5_forcasting
    WHERE id IS NOT NULL;
    ''')
    print('1. Silver Inventory Time-Shifted.')

    # 2. Rebuild Gold Food Waste Summary with REAL Join
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
    sales AS (
        SELECT 
            order_date_id AS date_id,
            product_id,
            store_id,
            SUM(quantity) AS total_sold
        FROM silver.silver_fact_sales
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
    LEFT JOIN silver.silver_dim_product p
        ON i.product_id = p.product_id
    ''')
    print('2. Gold Food Waste rebuilt with authentic overlap.')

    conn.close()
except Exception as e:
    print(f'Error: {e}')
