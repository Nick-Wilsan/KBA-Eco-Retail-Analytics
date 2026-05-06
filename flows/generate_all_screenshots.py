import pandas as pd
import duckdb
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics
import matplotlib.pyplot as plt
from sklearn.ensemble import IsolationForest
import warnings
import logging

# Menyembunyikan warning dan log
warnings.filterwarnings('ignore')
logger = logging.getLogger('cmdstanpy')
logger.addHandler(logging.NullHandler())
logger.propagate = False
logger.setLevel(logging.CRITICAL)

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

def run_all():
    print("="*80)
    print("MENGHUBUNGKAN KE DUCKDB...")
    conn = duckdb.connect("data/warehouse.duckdb", read_only=True)
    
    # ---------------------------------------------------------
    # PROPHET FORECASTING (SS 1, SS 2, SS 3)
    # ---------------------------------------------------------
    print("="*80)
    print("MEMPROSES DATA FORECASTING PROPHET...")
    df_sales = conn.execute("SELECT * FROM silver.silver_fact_sales").fetchdf()
    
    df_agg = df_sales.groupby('order_date_id')['quantity'].sum().reset_index()
    df_agg = df_agg.rename(columns={'order_date_id': 'ds', 'quantity': 'y'})
    df_agg['ds'] = pd.to_datetime(df_agg['ds'])
    
    min_date = df_agg['ds'].min()
    max_date = df_agg['ds'].max()
    all_dates = pd.date_range(start=min_date, end=max_date, freq='D')
    df_all_dates = pd.DataFrame({'ds': all_dates})
    df_prophet = df_all_dates.merge(df_agg, on='ds', how='left').fillna({'y': 0})
    
    model_ml = Prophet(
        changepoint_prior_scale=0.08,
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False,
        interval_width=0.80 
    )
    model_ml.fit(df_prophet)
    
    future = model_ml.make_future_dataframe(periods=7)
    forecast = model_ml.predict(future)
    
    fig1 = model_ml.plot(forecast)
    plt.title('Prophet Demand Forecast (Actual vs Predicted, 7 Days Future)')
    plt.xlabel('Date')
    plt.ylabel('Demand Quantity')
    plt.savefig('SS_1_Prophet_Forecast.png', bbox_inches='tight')
    plt.close(fig1)
    
    fig2 = model_ml.plot_components(forecast)
    plt.savefig('SS_3_Prophet_Components.png', bbox_inches='tight')
    plt.close(fig2)

    total_days = (max_date - min_date).days
    initial_days = max(30, int(total_days * 0.5))
    df_cv = cross_validation(model_ml, initial=f'{initial_days} days', period='30 days', horizon='30 days', disable_tqdm=True)
    df_p = performance_metrics(df_cv)
    mape = df_p['mape'].mean() * 100
    
    print("\n--- SS 2: OUTPUT TERMINAL EVALUASI WALK-FORWARD CROSS-VALIDATION ---")
    print("Evaluasi Model Prophet menggunakan Walk-Forward Cross-Validation...")
    print(f"Metrics dari cross validation (rata-rata):")
    print(df_p[['horizon', 'mse', 'rmse', 'mae', 'mape', 'mdape', 'coverage']].head())
    print("-" * 65)
    print(f">>> FINAL EVALUATION: MAPE Model Prophet = {mape:.2f}% <<<")
    print("-" * 65)

    # ---------------------------------------------------------
    # ISOLATION FOREST ANOMALY DETECTION (SS 5, SS 6)
    # ---------------------------------------------------------
    print("\n" + "="*80)
    print("MEMPROSES DATA ANOMALI COLD CHAIN...")
    df_cold = conn.execute("SELECT * FROM silver.silver_fact_cold_chain").fetchdf()
    
    df_cold['telemetry_timestamp'] = pd.to_datetime(df_cold['telemetry_timestamp'])
    df_cold = df_cold.sort_values(['device_id', 'telemetry_timestamp'])
    df_cold['duration_minutes'] = df_cold.groupby('device_id')['telemetry_timestamp'].diff().dt.total_seconds() / 60.0
    df_cold['duration_minutes'] = df_cold['duration_minutes'].fillna(0)
    
    features = ['temperature_c', 'duration_minutes']
    X = df_cold[features].fillna(0)
    
    model_if = IsolationForest(contamination=0.05, random_state=42)
    df_cold['anomaly_score'] = model_if.fit_predict(X)
    
    def get_anomaly_type(row):
        if row['anomaly_score'] == -1 and row['duration_minutes'] > 30:
            return 'Equipment Breach'
        elif row['anomaly_score'] == -1 and row['duration_minutes'] <= 30:
            return 'Operational Error'
        else:
            return 'Normal'

    df_cold['anomaly_type_dominant'] = df_cold.apply(get_anomaly_type, axis=1)
    
    # Menampilkan data apa adanya (hanya me-rename kolom untuk tampilan agar sesuai instruksi jika memungkinkan)
    # Tanpa membuat data palsu seperti 'zone'
    df_ss5 = df_cold[['telemetry_timestamp', 'device_id', 'temperature_c', 'anomaly_score', 'anomaly_type_dominant']].copy()
    df_ss5 = df_ss5.rename(columns={
        'telemetry_timestamp': 'timestamp',
        'device_id': 'sensor_id',
        'temperature_c': 'temperature'
    })
    
    print("\n--- SS 5: SAMPEL OUTPUT KLASIFIKASI ISOLATION FOREST ---")
    # Langsung print 20 baris pertama dari data asli yang sudah disortir berdasarkan waktu
    # Dari hasil sebelumnya, 20 baris pertama sudah memuat campuran Normal dan Anomali
    print(df_ss5.head(20))
    print("-" * 80)
    
    # SS 6: Visualisasi
    plt.figure(figsize=(12, 6))
    normal_data = df_cold[df_cold['anomaly_score'] == 1]
    anomaly_data = df_cold[df_cold['anomaly_score'] == -1]
    
    plt.scatter(normal_data['telemetry_timestamp'], normal_data['temperature_c'], color='blue', label='Normal', alpha=0.5, s=20)
    plt.scatter(anomaly_data['telemetry_timestamp'], anomaly_data['temperature_c'], color='red', label='Anomaly', alpha=0.8, s=30)
    
    # Menambahkan garis threshold suhu (-2C s/d 8C)
    plt.axhline(y=8, color='green', linestyle='--', linewidth=2, label='Upper Threshold (8°C)')
    plt.axhline(y=-2, color='green', linestyle='--', linewidth=2, label='Lower Threshold (-2°C)')
    
    plt.xlabel('Timestamp')
    plt.ylabel('Temperature (°C)')
    plt.title('Isolation Forest Anomaly Detection: Temperature vs Time')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.savefig('SS_6_Anomaly_Visualization.png', bbox_inches='tight')
    plt.close()
    
    print("\nSemua gambar (SS_1, SS_3, SS_6) telah berhasil disimpan sebagai PNG di folder proyek!")
    print("="*80)

if __name__ == "__main__":
    run_all()