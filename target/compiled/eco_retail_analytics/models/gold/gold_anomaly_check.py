import pandas as pd
from sklearn.ensemble import IsolationForest
import matplotlib.pyplot as plt

def model(dbt, session):
    dbt.config(
        materialized="table",
        # alias="gold_mart_cold_chain_compliance",
        packages=["scikit-learn", "pandas", "matplotlib"]
    )

    # 1. Membaca Data: Mengambil data dari tabel silver_fact_cold_chain di DuckDB
    df = dbt.ref("silver_fact_cold_chain").to_df()

    if df.empty:
        return pd.DataFrame()

    # Menyiapkan data dan menghitung duration_minutes (selisih waktu antar pembacaan sensor)
    df['telemetry_timestamp'] = pd.to_datetime(df['telemetry_timestamp'])
    df = df.sort_values(['device_id', 'telemetry_timestamp'])
    
    # Menghitung durasi dalam menit sejak pembacaan terakhir untuk setiap perangkat
    df['duration_minutes'] = df.groupby('device_id')['telemetry_timestamp'].diff().dt.total_seconds() / 60.0
    df['duration_minutes'] = df['duration_minutes'].fillna(0)
    
    # Menghitung delta_temp_per_minute untuk mendeteksi kenaikan suhu gradual
    df['temp_diff'] = df.groupby('device_id')['temperature_c'].diff().fillna(0)
    df['delta_temp_per_minute'] = df.apply(
        lambda row: row['temp_diff'] / row['duration_minutes'] if row['duration_minutes'] > 0 else 0, axis=1
    )

    # 2. Modeling: Melatih IsolationForest menggunakan fitur suhu, durasi, dan delta suhu
    features = ['temperature_c', 'duration_minutes', 'delta_temp_per_minute']
    X = df[features].fillna(0)
    
    # Melatih IsolationForest dengan contamination=0.05 sesuai PRD
    model_if = IsolationForest(contamination=0.05, random_state=42)
    # anomaly_score: -1 berarti anomali, 1 berarti normal
    df['anomaly_score'] = model_if.fit_predict(X)

    # 3. Post-Processing (CRITICAL): Menambahkan kolom tipe anomali dominan
    # Menerapkan business rules dari Laporan Minggu 3:
    # - Equipment Breach: anomali & durasi > 30 menit & delta_temp > 0.5 (gradual)
    # - Operational Error: anomali & (durasi <= 30 menit ATAU delta_temp <= 0.5)
    def get_anomaly_type(row):
        if row['anomaly_score'] == -1:
            if row['duration_minutes'] > 30 and row['delta_temp_per_minute'] > 0.5:
                return 'Equipment Breach'
            else:
                return 'Operational Error'
        return 'Normal'

    df['anomaly_type_dominant'] = df.apply(get_anomaly_type, axis=1)
    
    # Menambahkan flag metrics untuk dibaca oleh dbt sql downstream (gold_mart_cold_chain_compliance.sql)
    df['equipment_breach'] = df['anomaly_type_dominant'].apply(lambda x: 1 if x == 'Equipment Breach' else 0)
    df['compliance_rate_pct'] = df['anomaly_score'].apply(lambda x: 100.0 if x == 1 else 0.0)

    # 4. Console Output: Mencetak 20 baris pertama yang menunjukkan hasil prediksi
    # Menggunakan alias sensor_id dan temperature agar sesuai dengan permintaan, meski aslinya device_id & temperature_c
    output_df = df[['telemetry_timestamp', 'device_id', 'temperature_c', 'anomaly_type_dominant']].copy()
    output_df.rename(columns={'telemetry_timestamp': 'timestamp', 'device_id': 'sensor_id', 'temperature_c': 'temperature'}, inplace=True)
    print("--- First 20 rows of Anomaly Detection ---")
    print(output_df.head(20))

    # 5. Visualization: Membuat scatter plot Suhu vs Waktu
    plt.figure(figsize=(10, 6))
    normal_data = df[df['anomaly_score'] == 1]
    anomaly_data = df[df['anomaly_score'] == -1]
    
    # Kode warna: Biru untuk Normal, Merah untuk Anomali
    plt.scatter(normal_data['telemetry_timestamp'], normal_data['temperature_c'], color='blue', label='Normal', alpha=0.5)
    plt.scatter(anomaly_data['telemetry_timestamp'], anomaly_data['temperature_c'], color='red', label='Anomaly', alpha=0.5)
    plt.xlabel('Time')
    plt.ylabel('Temperature (C)')
    plt.title('Isolation Forest Anomaly Detection: Temperature vs Time')
    plt.legend()
    plt.show()

    # 6. Database Write-back: Dataframe disimpan ke DuckDB sebagai gold.gold_mart_cold_chain_compliance
    # Ini sudah diatur oleh dbt.config di awal fungsi (alias='gold_mart_cold_chain_compliance')
    return df


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args, **kwargs):
    refs = {"silver_fact_cold_chain": "\"warehouse\".\"silver\".\"silver_fact_cold_chain\""}
    key = '.'.join(args)
    version = kwargs.get("v") or kwargs.get("version")
    if version:
        key += f".v{version}"
    dbt_load_df_function = kwargs.get("dbt_load_df_function")
    return dbt_load_df_function(refs[key])


def source(*args, dbt_load_df_function):
    sources = {}
    key = '.'.join(args)
    return dbt_load_df_function(sources[key])


config_dict = {}
meta_dict = {}


class config:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get(key, default=None):
        return config_dict.get(key, default)

    @staticmethod
    def meta_get(key, default=None):
        return meta_dict.get(key, default)

class this:
    """dbt.this() or dbt.this.identifier"""
    database = "warehouse"
    schema = "gold"
    identifier = "gold_anomaly_check"
    
    def __repr__(self):
        return '"warehouse"."gold"."gold_anomaly_check"'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------


