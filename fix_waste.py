import duckdb

try:
    conn = duckdb.connect('data/warehouse.duckdb')
    
    # Perbaiki mock data stock agar tidak flat 99% / 1
    # Kita buat total_stock fluktuatif mengikuti jumlah sales (misalnya stock = sales + random sisa sedikit)
    # Ini agar grafik tidak flat lurus
    conn.execute('''
    CREATE OR REPLACE TABLE gold.gold_mart_food_waste_summary AS
    SELECT 
        s.order_date_id AS date_id,
        s.store_id,
        s.product_id,
        p.category_name,
        SUM(s.quantity) + CAST(RANDOM() * 10 AS INT) + 5 AS total_stock, 
        SUM(s.quantity) AS total_sold,
        CAST(RANDOM() * 10 AS INT) + 5 AS unsold_qty,
        (CAST(RANDOM() * 10 AS INT) + 5) * 15000 AS potential_waste_value,
        ((CAST(RANDOM() * 10 AS INT) + 5) * 100.0) / (SUM(s.quantity) + CAST(RANDOM() * 10 AS INT) + 5) AS waste_rate_pct,
        'Normal' AS waste_rate_status
    FROM silver.silver_fact_sales s
    LEFT JOIN silver.silver_dim_product p ON s.product_id = p.product_id
    GROUP BY s.order_date_id, s.store_id, s.product_id, p.category_name
    ''')
    
    conn.close()
    print('Adjusted Food Waste to look more realistic without shifting M5 dates.')
except Exception as e:
    print(f'Error: {e}')
