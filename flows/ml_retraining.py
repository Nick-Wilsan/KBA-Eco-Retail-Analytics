import os
import duckdb
import pandas as pd
from datetime import datetime
from prefect import flow, task
from prophet import Prophet
from dotenv import load_dotenv

# Load konfigurasi
load_dotenv()
DB_PATH = os.getenv("DUCKDB_PATH", "data/warehouse.duckdb")
# Sesuaikan skema: gunakan 'warehouse_gold' jika Anda pakai Solusi 1 sebelumnya, atau 'gold' jika Solusi 2
SCHEMA_NAME = "warehouse_gold" 

@task(name="1. Ekstraksi Data Historis (Gold Layer)", retries=2)
def extract_training_data():
    """Mengambil data tren penjualan historis untuk dilatih oleh model."""
    print("Mengambil data dari gold_mart_demand_forecast...")
    conn = duckdb.connect(DB_PATH, read_only=True) # Read-only karena hanya SELECT
    
    try:
        # Prophet secara spesifik membutuhkan kolom bernama 'ds' (tanggal) dan 'y' (target/nilai)
        query = f"""
            SELECT 
                date_id AS ds, 
                SUM(daily_sales_qty) AS y
            FROM {SCHEMA_NAME}.gold_mart_demand_forecast
            WHERE date_id IS NOT NULL
            GROUP BY date_id
            ORDER BY date_id ASC
        """
        df = conn.execute(query).fetchdf()
        return df
    finally:
        conn.close()

@task(name="2. Retraining Model Prophet & Forecasting", log_prints=True)
def train_and_predict(df_history):
    """Melatih ulang model dengan data terbaru dan membuat prediksi 7 hari ke depan."""
    print(f"Mulai melatih model dengan {len(df_history)} baris data historis...")
    
    # Inisiasi dan latih model Prophet
    model = Prophet(daily_seasonality=True, yearly_seasonality=True)
    model.fit(df_history)
    
    # Buat dataframe untuk prediksi 7 hari ke depan
    future = model.make_future_dataframe(periods=7)
    forecast = model.predict(future)
    
    # Filter hanya mengambil 7 hari ke depan (hasil prediksi murni) dan kolom yang dibutuhkan
    predictions = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(7)
    
    # Tambahkan metadata kapan prediksi ini dibuat
    predictions['prediction_generated_at'] = datetime.now()
    
    print("Forecasting selesai.")
    return predictions

@task(name="3. Simpan Hasil Prediksi ke Gold Layer", retries=2)
def load_predictions_to_gold(predictions_df):
    """Menyimpan hasil forecasting kembali ke Data Warehouse (Gold Layer)."""
    print("Menyimpan hasil prediksi ke DuckDB...")
    # PENTING: Jangan gunakan read_only=True di sini karena kita akan CREATE dan INSERT
    conn = duckdb.connect(DB_PATH) 
    table_name = f"{SCHEMA_NAME}.ml_demand_predictions"
    
    try:
        # 1. Pastikan tabel penampung prediksi sudah ada
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                ds TIMESTAMP,
                yhat DOUBLE,
                yhat_lower DOUBLE,
                yhat_upper DOUBLE,
                prediction_generated_at TIMESTAMP
            )
        """)
        
        # 2. Insert data baru
        # Fitur ajaib DuckDB: Bisa langsung Query dari Pandas DataFrame yang ada di memory Python!
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM predictions_df")
        
        total_rows = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"✔ Berhasil! Total histori prediksi di {table_name} saat ini: {total_rows} baris.")
    finally:
        conn.close()

@flow(name="ML Retraining Flow - Demand Forecasting")
def flow_ml_retraining():
    """Orkestrasi utama Pipeline Machine Learning"""
    print("=== Memulai Pipeline ML Retraining ===")
    
    df_history = extract_training_data()
    
    if df_history.empty:
        print("Data historis kosong. Pipeline dihentikan.")
        return
        
    predictions_df = train_and_predict(df_history)
    load_predictions_to_gold(predictions_df)
    
    print("=== Pipeline ML Selesai ===")

if __name__ == "__main__":
    # Cara Prefect mengatur jadwal (Schedule) otomatis tiap minggu (contoh: Tiap Senin jam 02:00 pagi)
    # Jika hanya ingin menjalankan sekali (manual), cukup panggil flow_ml_retraining()
    
    flow_ml_retraining() # <-- Gunakan ini untuk testing manual
    
    # flow_ml_retraining.serve(
    #     name="weekly-demand-forecast",
    #     cron="0 2 * * 1", # Jadwal CRON: Menit 0, Jam 2, Tiap Bulan, Tiap Hari, Hari Senin(1)
    #     tags=["machine-learning", "gold-layer"]
    # ) # <-- Matikan ini untuk testing manual, aktifkan untuk jadwal otomatis
    
