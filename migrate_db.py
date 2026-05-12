import duckdb

try:
    conn = duckdb.connect('data/warehouse.duckdb')
    
    print('Copying ml_demand_predictions to gold schema...')
    try:
        conn.execute('CREATE OR REPLACE TABLE gold.ml_demand_predictions AS SELECT * FROM warehouse_gold.ml_demand_predictions;')
        print('Copied successfully.')
    except Exception as e:
        print(f'Error copying: {e}')

    print('Dropping old schemas...')
    conn.execute('DROP SCHEMA IF EXISTS warehouse_gold CASCADE;')
    conn.execute('DROP SCHEMA IF EXISTS warehouse_silver CASCADE;')
    conn.execute('DROP SCHEMA IF EXISTS warehouse_bronze CASCADE;')
    print('Schemas dropped.')
        
    conn.close()
except Exception as e:
    print(f'Error: {e}')
