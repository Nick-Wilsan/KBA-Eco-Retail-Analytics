
  
    import pandas as pd
from sklearn.ensemble import IsolationForest
import matplotlib.pyplot as plt

def model(dbt, session):
    dbt.config(
        materialized="table",
        alias="gold_mart_cold_chain_compliance",
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

    # 2. Modeling: Melatih IsolationForest menggunakan fitur suhu dan durasi
    features = ['temperature_c', 'duration_minutes']
    X = df[features].fillna(0)
    
    # Melatih IsolationForest dengan contamination=0.05 sesuai permintaan untuk mendeteksi anomali
    model_if = IsolationForest(contamination=0.05, random_state=42)
    # anomaly_score: -1 berarti anomali, 1 berarti normal
    df['anomaly_score'] = model_if.fit_predict(X)

    # 3. Post-Processing (CRITICAL): Menambahkan kolom tipe anomali dominan
    # Menerapkan business rules: 
    # Jika anomali dan durasi > 30 menit, maka 'Equipment Breach'
    # Jika anomali dan durasi <= 30 menit, maka 'Operational Error'
    # Jika tidak, maka 'Normal'
    def get_anomaly_type(row):
        if row['anomaly_score'] == -1 and row['duration_minutes'] > 30:
            return 'Equipment Breach'
        elif row['anomaly_score'] == -1 and row['duration_minutes'] <= 30:
            return 'Operational Error'
        else:
            return 'Normal'

    df['anomaly_type_dominant'] = df.apply(get_anomaly_type, axis=1)

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
    identifier = "gold_mart_cold_chain_compliance"
    
    def __repr__(self):
        return '"warehouse"."gold"."gold_mart_cold_chain_compliance"'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------




def materialize(df, con):
    try:
        import pyarrow
        pyarrow_available = True
    except ImportError:
        pyarrow_available = False
    finally:
        if pyarrow_available and isinstance(df, pyarrow.Table):
            # https://github.com/duckdb/duckdb/issues/6584
            import pyarrow.dataset
    tmp_name = '__dbt_python_model_df_' + 'gold_mart_cold_chain_compliance__dbt_tmp'
    con.register(tmp_name, df)
    con.execute('create table "warehouse"."gold"."gold_mart_cold_chain_compliance__dbt_tmp" as select * from ' + tmp_name)
    con.unregister(tmp_name)

  