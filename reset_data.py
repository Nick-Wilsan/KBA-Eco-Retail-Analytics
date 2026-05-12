import duckdb
import os
import pandas as pd

try:
    print('Connecting to DB...')
    conn = duckdb.connect('data/warehouse.duckdb')
    
    print('Reading raw real IoT data...')
    real_csv = 'data/iot_telemetry_cleaned.csv'
    if os.path.exists(real_csv):
        df_real = pd.read_csv(real_csv)
        df_real = df_real.rename(columns={'device_mac': 'device_mac', 'humidity_pct': 'humidity_pct', 'temp_celsius': 'temp_celsius'})
        
        # Simpan kembali sebagai iot_telemetry.csv murni tanpa sintetik
        df_real.to_csv('data/raw/iot_telemetry.csv', index=False)
        print('Overwritten data/raw/iot_telemetry.csv with pure real data.')
        
        print('Re-ingesting bronze.iot_telemetry...')
        conn.execute('''
            CREATE OR REPLACE TABLE bronze.iot_telemetry AS
            SELECT
                *,
                current_timestamp AS _loaded_at,
                'data/raw/iot_telemetry.csv' AS _source_file
            FROM read_csv_auto('data/raw/iot_telemetry.csv', timestampformat='%Y-%m-%d %H:%M:%S.%f+07');
        ''')
        count = conn.execute('SELECT COUNT(*) FROM bronze.iot_telemetry').fetchone()[0]
        print(f'bronze.iot_telemetry rebuilt with {count} rows.')
    else:
        print(f'Error: Could not find {real_csv}')
        
    conn.close()
except Exception as e:
    print(f'Error: {e}')
