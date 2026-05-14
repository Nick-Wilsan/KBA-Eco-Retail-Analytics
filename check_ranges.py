import duckdb
try:
    conn = duckdb.connect('data/warehouse.duckdb', read_only=True)
    print('Checking Data Ranges...')
    print('Inventory:')
    print(conn.execute('SELECT MIN(date_id), MAX(date_id) FROM silver.silver_fact_inventory').fetchdf())
    print('Sales:')
    print(conn.execute('SELECT MIN(order_date_id), MAX(order_date_id) FROM silver.silver_fact_sales').fetchdf())
    conn.close()
except Exception as e:
    print(f'Error: {e}')
