import duckdb

try:
    conn = duckdb.connect('data/warehouse.duckdb', read_only=True)
    
    print('--- Schemas ---')
    schemas = conn.execute('SHOW SCHEMAS').fetchall()
    for s in schemas:
        print(s[0])
        
    print('\n--- Tables in warehouse_* vs standard ---')
    tables = conn.execute('''
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_schema IN ('bronze', 'silver', 'gold', 'warehouse_bronze', 'warehouse_silver', 'warehouse_gold')
        ORDER BY table_schema, table_name
    ''').fetchall()
    for t in tables:
        print(f'{t[0]}.{t[1]}')
        
    conn.close()
except Exception as e:
    print(f'Error: {e}')
