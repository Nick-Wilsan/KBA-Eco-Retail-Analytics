import duckdb
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

def run_task6():
    print("="*80)
    print("TASK 6: DATABASE WRITE-BACK (GOLD LAYER RESULTS)")
    print("="*80)
    
    conn = duckdb.connect("data/warehouse.duckdb", read_only=True)
    
    print("\n[6.1] Tabel Demand Forecast (Menampilkan kolom yhat, lower_bound, upper_bound)")
    df_forecast = conn.execute("SELECT ds, forecast_qty, lower_bound, upper_bound, historical_qty FROM gold.gold_mart_demand_forecast LIMIT 5;").fetchdf()
    print(df_forecast)
    print("-" * 80)

    print("\n[6.2] Tabel Cold Chain Compliance (Menampilkan hasil tipe anomali)")
    df_compliance = conn.execute("SELECT telemetry_timestamp, device_id, temperature_c, duration_minutes, anomaly_type_dominant FROM gold.gold_mart_cold_chain_compliance LIMIT 5;").fetchdf()
    print(df_compliance)
    print("=" * 80 + "\n")

if __name__ == "__main__":
    run_task6()