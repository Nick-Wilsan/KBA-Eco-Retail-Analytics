import duckdb

try:
    conn = duckdb.connect('data/warehouse.duckdb', read_only=True)
    
    print('--- Check Store Dimension ---')
    print(conn.execute('SELECT * FROM silver.silver_dim_store LIMIT 5').fetchdf())
    
    print('\n--- Check Product Dimension ---')
    print(conn.execute('SELECT * FROM silver.silver_dim_product LIMIT 5').fetchdf())
    
    print('\n--- Check Category in Gold ---')
    print(conn.execute('SELECT category_name, SUM(unsold_qty) FROM gold.gold_mart_food_waste_summary GROUP BY category_name').fetchdf())

    print('\n--- Check Store in Gold ---')
    print(conn.execute('SELECT store_id, SUM(unsold_qty) FROM gold.gold_mart_food_waste_summary GROUP BY store_id LIMIT 5').fetchdf())

    conn.close()
except Exception as e:
    print(f'Error: {e}')
