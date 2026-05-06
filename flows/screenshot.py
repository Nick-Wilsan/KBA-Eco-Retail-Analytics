import duckdb
import pandas as pd

# Setting pandas agar tabel di terminal tidak terpotong (tampil memanjang)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

# Sambungkan ke database DuckDB Anda
# Gunakan read_only=True agar tidak bentrok dengan proses lain
conn = duckdb.connect("data/warehouse.duckdb", read_only=True)

print("="*80)
print("1. TABEL DEMAND FORECAST (Layer Gold)")
print("="*80)
# Menampilkan 5 baris pertama untuk melihat forecast_qty, lower_bound, upper_bound
# Catatan: Jika error, ganti 'gold.' menjadi 'warehouse_gold.'
df_forecast = conn.execute("SELECT * FROM gold.gold_mart_demand_forecast LIMIT 5;").fetchdf()
print(df_forecast)
print("\n")

print("="*80)
print("2. TABEL COLD CHAIN COMPLIANCE (Layer Gold)")
print("="*80)

# Menarik data dari tabel Mart yang sudah merangkum hasil AI
query = """
SELECT 
    date_id, 
    device_id, 
    equipment_breach_count, 
    compliance_rate_pct, 
    anomaly_type_dominant
FROM gold.gold_mart_cold_chain_compliance
WHERE equipment_breach_count > 0 -- Sengaja menampilkan yang ada anomalinya
LIMIT 5;
"""

df_compliance = conn.execute(query).fetchdf()
print(df_compliance)
print("\n")

print("="*80)
print("3. VOLUME DATA BRONZE LAYER")
print("="*80)
# Membuktikan bahwa data Instacart Grocery ditarik utuh (biasanya > 3 juta baris)
df_volume = conn.execute("SELECT COUNT(*) AS total_rows FROM bronze.instacart_grocery;").fetchdf()
print(df_volume)
print("\n")

conn.close()