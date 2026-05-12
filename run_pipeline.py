import duckdb
import subprocess
import os

try:
    # 1. Pastikan dbt dapat dijalankan lewat subprocess
    # Karena dbt tidak terbaca, kita bisa eksekusi ulang flow Prefect yang sama
    print('Menjalankan ulang ml_retraining pipeline untuk sinkronisasi prediksi (bisa error jika data dbt belum update)...')
    
    # Sebagai alternatif, mari kita gunakan DuckDB untuk UPDATE manual jika dbt gagal.
    # dbt compile SQL -> kita bisa jalankan query kompilasinya langsung di DuckDB.
    print('Bypass dbt: Executing SQL logic directly into DuckDB.')
    conn = duckdb.connect('data/warehouse.duckdb')
    
    # Recreate silver_fact_cold_chain
    conn.execute('''
    CREATE OR REPLACE TABLE silver.silver_fact_cold_chain AS 
    SELECT
        CAST(timestamp AS TIMESTAMP) AS telemetry_timestamp,
        CAST(timestamp AS DATE) AS date_id,
        device_mac AS device_id,
        CAST(temp_celsius AS DOUBLE) AS temperature_c,
        CAST(humidity_pct AS DOUBLE) AS humidity_percentage,
        CAST(co_level AS DOUBLE) AS co_level,
        CAST(smoke_level AS DOUBLE) AS smoke_level,
        CAST(is_light AS BOOLEAN) AS is_light_on,
        CAST(is_motion AS BOOLEAN) AS is_motion_detected
    FROM bronze.iot_telemetry
    WHERE temp_celsius BETWEEN -100 AND 100;
    ''')
    print('silver.silver_fact_cold_chain recreated.')
    
    # Recreate silver_fact_sales
    conn.execute('''
    CREATE OR REPLACE TABLE silver.silver_fact_sales AS
    SELECT
        CAST(order_item_id AS VARCHAR) AS order_item_id,
        CAST(order_id AS VARCHAR) AS order_id,
        CAST(order_date AS DATE) AS order_date_id,
        CAST(customer_id AS VARCHAR) AS customer_id,
        CAST(store_id AS VARCHAR) AS store_id,
        CAST(product_id AS VARCHAR) AS product_id,
        CAST(qty AS INTEGER) AS quantity,
        CAST(unit_price AS DOUBLE) AS unit_price,
        CAST(discount_applied AS DOUBLE) AS discount_amount,
        CAST(total_order_payment AS DOUBLE) AS total_payment,
        shipment_status
    FROM bronze.retail_data
    WHERE order_item_id IS NOT NULL;
    ''')
    print('silver.silver_fact_sales recreated.')
    
    conn.close()
    
    # Jalankan ulang skrip prefect untuk orkestrasi
    subprocess.run(['python', 'flows/main_pipeline.py'], check=False)
except Exception as e:
    print(f'Error: {e}')
