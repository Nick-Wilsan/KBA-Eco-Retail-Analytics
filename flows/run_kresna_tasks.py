import pandas as pd
import duckdb
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics
import matplotlib.pyplot as plt
from sklearn.ensemble import IsolationForest
import warnings
warnings.filterwarnings('ignore')

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

def run_tasks():
    print("="*80)
    print("Connecting to DuckDB...")
    conn = duckdb.connect("data/warehouse.duckdb", read_only=True)
    
    # ---------------------------------------------------------
    # PROPHET FORECASTING (Task 1, 2, 3)
    # ---------------------------------------------------------
    print("="*80)
    print("Running Prophet Forecasting...")
    df_sales = conn.execute("SELECT * FROM silver.silver_fact_sales").fetchdf()
    
    # Preprocessing
    df_agg = df_sales.groupby('order_date_id')['quantity'].sum().reset_index()
    df_agg = df_agg.rename(columns={'order_date_id': 'ds', 'quantity': 'y'})
    df_agg['ds'] = pd.to_datetime(df_agg['ds'])
    
    min_date = df_agg['ds'].min()
    max_date = df_agg['ds'].max()
    all_dates = pd.date_range(start=min_date, end=max_date, freq='D')
    df_all_dates = pd.DataFrame({'ds': all_dates})
    df_prophet = df_all_dates.merge(df_agg, on='ds', how='left').fillna({'y': 0})
    
    model_ml = Prophet(
        changepoint_prior_scale=0.10,
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False
    )
    model_ml.fit(df_prophet)
    
    # Cross Validation
    total_days = (max_date - min_date).days
    initial_days = max(30, int(total_days * 0.5))
    df_cv = cross_validation(model_ml, initial=f'{initial_days} days', period='30 days', horizon='30 days')
    df_p = performance_metrics(df_cv)
    mape = df_p['mape'].mean() * 100
    print("\n--- TASK 2: Log Terminal MAPE ---")
    print(f"Prophet Cross-Validation MAPE: {mape:.2f}%")
    print("-" * 33)
    
    future = model_ml.make_future_dataframe(periods=30)
    forecast = model_ml.predict(future)
    
    print("\nSaving Prophet plots...")
    fig1 = model_ml.plot(forecast)
    plt.title('Prophet Demand Forecast (Actual vs Predicted)')
    plt.savefig('task_1_prophet_forecast.png', bbox_inches='tight')
    plt.close(fig1)
    
    fig2 = model_ml.plot_components(forecast)
    plt.savefig('task_3_prophet_components.png', bbox_inches='tight')
    plt.close(fig2)

    # ---------------------------------------------------------
    # ISOLATION FOREST ANOMALY DETECTION (Task 4, 5)
    # ---------------------------------------------------------
    print("="*80)
    print("Running Isolation Forest Anomaly Detection...")
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
        if row['anomaly_score'] == -1:
            # Equipment Breach: suhu naik di atas rentang normal (>=20°C) → alat pendingin bermasalah
            if row['temperature_c'] >= 20:
                return 'Equipment Breach'
            # Operational Error: suhu turun tidak wajar (<20°C) → kemungkinan human error
            else:
                return 'Operational Error'
        return 'Normal'

    df_cold['anomaly_type_dominant'] = df_cold.apply(get_anomaly_type, axis=1)

    print("\n--- DISTRIBUSI LABEL ANOMALI ---")
    dist = df_cold['anomaly_type_dominant'].value_counts()
    dist_pct = df_cold['anomaly_type_dominant'].value_counts(normalize=True).mul(100).round(2)
    for label in dist.index:
        print(f"  {label}: {dist[label]} rows ({dist_pct[label]:.2f}%)")
    print("-" * 40)

    output_df = df_cold[['date_id', 'device_id', 'temperature_c', 'anomaly_type_dominant']].copy()
    output_df.rename(columns={'temperature_c': 'avg_temp_c'}, inplace=True)

    print("\n--- TASK 4: Output Klasifikasi Isolation Forest (First 20 Rows) ---")
    print(output_df.head(20))
    print("-" * 65)
    
    # Task 5 visualization
    print("\nSaving Isolation Forest scatter plot...")
    plt.figure(figsize=(10, 6))
    normal_data = df_cold[df_cold['anomaly_score'] == 1]
    anomaly_data = df_cold[df_cold['anomaly_score'] == -1]
    
    plt.scatter(normal_data['telemetry_timestamp'], normal_data['temperature_c'], color='blue', label='Normal', alpha=0.5)
    plt.scatter(anomaly_data['telemetry_timestamp'], anomaly_data['temperature_c'], color='red', label='Anomaly', alpha=0.5)
    plt.xlabel('Time')
    plt.ylabel('Temperature (C)')
    plt.title('Isolation Forest Anomaly Detection: Temperature vs Time')
    plt.legend()
    plt.savefig('task_5_anomaly_distribution.png', bbox_inches='tight')
    plt.close()
    
    print("="*80)
    print("All tasks completed successfully. Plots are saved as PNG files.")

if __name__ == "__main__":
    run_tasks()
