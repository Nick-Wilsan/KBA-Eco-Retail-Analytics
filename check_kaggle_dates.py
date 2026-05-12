import duckdb
import pandas as pd

try:
    conn = duckdb.connect('data/warehouse.duckdb', read_only=True)
    
    print('--- Data Ranges ---')
    queries = {
        'Sales (Retail Data)': 'SELECT MIN(_loaded_at), MAX(_loaded_at), MIN(order_date), MAX(order_date) FROM bronze.retail_data',
        'Sales (M5)': 'SELECT MIN(date), MAX(date) FROM bronze.m5_forcasting',
        'IoT Telemetry': 'SELECT MIN(timestamp), MAX(timestamp) FROM bronze.iot_telemetry',
        'Gold Mart Executive KPI': 'SELECT MIN(kpi_month), MAX(kpi_month) FROM gold.gold_mart_executive_kpi',
        'Gold Mart Food Waste Summary': 'SELECT MIN(date_id), MAX(date_id) FROM gold.gold_mart_food_waste_summary'
    }
    
    for k, q in queries.items():
        try:
            res = conn.execute(q).fetchall()
            print(f'{k}: {res}')
        except Exception as e:
            print(f'{k} Error: {e}')
            
    conn.close()
except Exception as e:
    print(f'Error: {e}')
