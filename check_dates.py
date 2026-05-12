import duckdb

try:
    print('Testing query data ranges...')
    conn = duckdb.connect('data/warehouse.duckdb', read_only=True)
    
    # Check Sales range
    sales_res = conn.execute("SELECT MIN(order_date_id), MAX(order_date_id) FROM silver.silver_fact_sales").fetchall()
    print(f'Sales Data Range: {sales_res}')

    # Check IoT range
    iot_res = conn.execute("SELECT MIN(date_id), MAX(date_id) FROM silver.silver_fact_cold_chain").fetchall()
    print(f'IoT Data Range: {iot_res}')
    
    conn.close()

except Exception as e:
    print(f'Error: {e}')
