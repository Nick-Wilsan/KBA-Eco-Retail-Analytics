"""
JALANKAN SCRIPT INI SETELAH MENUTUP METABASE.

Step yang dilakukan:
1. Ingest data/raw/iot_telemetry.csv (real + sintetis) ke bronze.iot_telemetry
2. Rebuild silver.silver_fact_cold_chain via dbt
3. Rebuild gold_anomaly_check + gold_mart_cold_chain_compliance via dbt
4. Rebuild gold_mart_executive_kpi via dbt
5. Verifikasi hasilnya
"""

import os
import shutil
import subprocess
import duckdb

DB_PATH  = "data/warehouse.duckdb"
CSV_PATH = "data/raw/iot_telemetry.csv"

def find_dbt():
    dbt = shutil.which("dbt")
    if dbt:
        return dbt
    fallback = os.path.join(
        os.environ.get("LOCALAPPDATA", ""),
        "Packages", "PythonSoftwareFoundation.Python.3.13_qbz5n2kfra8p0",
        "LocalCache", "local-packages", "Python313", "Scripts", "dbt.exe"
    )
    if os.path.exists(fallback):
        return fallback
    raise FileNotFoundError("dbt tidak ditemukan. Pastikan dbt-core sudah terinstall.")

def run_dbt(dbt_exe, select):
    print(f"\ndbt run --select {select}")
    result = subprocess.run(
        [dbt_exe, "run", "--select", select],
        capture_output=True, text=True
    )
    print(result.stdout[-3000:] if len(result.stdout) > 3000 else result.stdout)
    if result.returncode != 0:
        print(result.stderr[-2000:])
        raise RuntimeError(f"dbt run failed untuk: {select}")
    return True

# ─── STEP 1: Bronze Ingest ────────────────────────────────────────────────────
print("=" * 60)
print("STEP 1: Bronze ingest dari CSV...")
print("=" * 60)

assert os.path.exists(CSV_PATH), f"CSV tidak ditemukan: {CSV_PATH}\nJalankan dulu: python generate_cold_chain_synthetic.py"

con = duckdb.connect(DB_PATH)
con.execute(f"""
    CREATE OR REPLACE TABLE bronze.iot_telemetry AS
    SELECT *, current_timestamp AS _loaded_at, '{CSV_PATH}' AS _source_file
    FROM read_csv_auto('{CSV_PATH}')
""")
count = con.execute("SELECT COUNT(*) FROM bronze.iot_telemetry").fetchone()[0]
r = con.execute("SELECT MIN(timestamp), MAX(timestamp) FROM bronze.iot_telemetry").fetchone()
print(f"bronze.iot_telemetry: {count:,} rows")
print(f"Timestamp range: {r[0]} s/d {r[1]}")
con.close()

# ─── STEP 2–4: dbt rebuild cold chain pipeline ────────────────────────────────
print("\n" + "=" * 60)
print("STEP 2-4: dbt rebuild cold chain pipeline...")
print("=" * 60)

DBT_EXE = find_dbt()

run_dbt(DBT_EXE, "stg_iot_telemetry silver_fact_cold_chain")
run_dbt(DBT_EXE, "gold_anomaly_check gold_mart_cold_chain_compliance gold_mart_executive_kpi")

# ─── STEP 5: Verifikasi ───────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("STEP 5: Verifikasi hasil...")
print("=" * 60)

con = duckdb.connect(DB_PATH, read_only=True)

r = con.execute("SELECT MIN(telemetry_timestamp), MAX(telemetry_timestamp), COUNT(*) FROM silver.silver_fact_cold_chain").fetchone()
print(f"\nsilver_fact_cold_chain : {r[2]:,} rows | {r[0]} s/d {r[1]}")

r = con.execute("SELECT MIN(date_id), MAX(date_id), COUNT(*) FROM gold.gold_mart_cold_chain_compliance").fetchone()
print(f"gold_mart_cold_chain   : {r[2]:,} rows | {r[0]} s/d {r[1]}")

print("\n=== Compliance per bulan (sample 1 tahun terakhir) ===")
import pandas as pd
df = con.execute("""
    SELECT DATE_TRUNC('month', date_id) AS bulan,
           ROUND(AVG(compliance_rate_pct), 2) AS avg_compliance,
           SUM(equipment_breach_count) AS total_breaches
    FROM gold.gold_mart_cold_chain_compliance
    WHERE date_id >= '2025-05-01'
    GROUP BY 1 ORDER BY 1
""").df()
print(df.to_string(index=False))

print("\n=== Interseksi semua tabel (untuk dashboard filter) ===")
fws = con.execute("SELECT MIN(date_id), MAX(date_id) FROM gold.gold_mart_food_waste_summary").fetchone()
ccc = con.execute("SELECT MIN(date_id), MAX(date_id) FROM gold.gold_mart_cold_chain_compliance").fetchone()
kpi = con.execute("SELECT MIN(kpi_month), MAX(kpi_month) FROM gold.gold_mart_executive_kpi").fetchone()
dfc = con.execute("SELECT CAST(MIN(ds) AS DATE), CAST(MAX(ds) AS DATE) FROM gold.gold_mart_demand_forecast").fetchone()

print(f"food_waste_summary   : {fws[0]} s/d {fws[1]}")
print(f"cold_chain_compliance: {ccc[0]} s/d {ccc[1]}")
print(f"executive_kpi        : {kpi[0]} s/d {kpi[1]}")
print(f"demand_forecast      : {dfc[0]} s/d {dfc[1]}")

overlap_start = max(fws[0], ccc[0], kpi[0], dfc[0])
overlap_end   = min(fws[1], ccc[1], kpi[1], dfc[1])
print(f"\n=> IRISAN KONSISTEN: {overlap_start} s/d {overlap_end}")
con.close()

print("\n" + "=" * 60)
print("SELESAI! Dashboard Metabase siap dengan data konsisten.")
print("=" * 60)
