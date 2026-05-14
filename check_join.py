import duckdb
try:
    conn = duckdb.connect('data/warehouse.duckdb', read_only=True)
    print('Checking Join Hit Rate...')
    q = '''
        SELECT COUNT(*) AS total_inventory_rows,
               COUNT(s.order_date_id) AS rows_with_sales
        FROM silver.silver_fact_inventory i
        LEFT JOIN silver.silver_fact_sales s
          ON i.date_id = s.order_date_id
          AND i.product_id = s.product_id
          AND i.store_id = s.store_id
    '''
    print(conn.execute(q).fetchdf())
    
    print('Sample Product IDs in Inventory:')
    print(conn.execute('SELECT DISTINCT product_id FROM silver.silver_fact_inventory LIMIT 3').fetchdf())
    
    print('Sample Product IDs in Sales:')
    print(conn.execute('SELECT DISTINCT product_id FROM silver.silver_fact_sales LIMIT 3').fetchdf())
    
    conn.close()
except Exception as e:
    print(f'Error: {e}')
