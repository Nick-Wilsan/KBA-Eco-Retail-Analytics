import duckdb

# Pastikan path ini sesuai dengan lokasi database DuckDB kamu
conn = duckdb.connect("data/warehouse.duckdb")

print("\n" + "="*60)
print("📸 SCREENSHOT 3: BUKTI LAYER SILVER (Data Bersih)")
print("Tabel: silver.silver_fact_inventory")
print("="*60)
try:
    # Memanggil 5 baris pertama dari tabel Silver
    df_silver = conn.execute("SELECT * FROM silver.silver_fact_inventory LIMIT 5;").df()
    print(df_silver.to_string(index=False))
except Exception as e:
    print(f"Gagal memuat tabel Silver: {e}")
    print("💡 Catatan: Pastikan kamu sudah menjalankan transformasi dbt ('dbt run') untuk tabel ini.")

print("\n" + "="*60)
print("📸 SCREENSHOT 4: BUKTI LAYER GOLD (Data Agregasi / KPI)")
print("Tabel: gold.gold_mart_food_waste_summary")
print("="*60)
try:
    # Memanggil 5 baris pertama dari tabel Gold
    df_gold = conn.execute("SELECT * FROM gold.gold_mart_food_waste_summary LIMIT 5;").df()
    print(df_gold.to_string(index=False))
except Exception as e:
    print(f"Gagal memuat tabel Gold: {e}")
    print("💡 Catatan: Pastikan kamu sudah menjalankan transformasi dbt ('dbt run') untuk tabel ini.")

conn.close()