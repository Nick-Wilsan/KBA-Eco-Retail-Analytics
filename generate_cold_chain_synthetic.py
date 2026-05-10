"""
Generate synthetic cold chain IoT data untuk mengisi gap historis.

Strategi:
- Real data (CSV): 2020-07-12 s/d 2020-07-20 (9 hari)
- silver_fact_cold_chain.sql menambahkan +5 YEAR +10 MONTH
- Sehingga real data di DB: 2026-05-12 s/d 2026-05-20

Target synthetic di DB: 2021-01-01 s/d 2026-05-11
=> Perlu timestamps di CSV: 2015-03-01 s/d 2020-07-11 (tepat sebelum real data)

Volume: 100 rows/device/hari × 3 devices × 1958 hari = ~587K rows
"""

import os
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone

np.random.seed(42)

# ─── Profil statistik per device (dari real data 9 hari) ──────────────────────
DEVICES = {
    'b8:27:eb:bf:9d:51': {
        'avg_temp': 22.28, 'std_temp': 0.482,
        'avg_hum': 50.814, 'std_hum': 1.889,
        'avg_co': 0.005560, 'std_co': 0.000559,
        'avg_lpg': 0.008306, 'std_lpg': 0.000599,
        'avg_smoke': 0.022288, 'std_smoke': 0.001720,
        'pct_light': 0.016, 'pct_motion': 0.0008,
        'breach_temp_range': (24.0, 30.0),  # suhu saat equipment breach
    },
    '00:0f:00:70:91:0a': {
        'avg_temp': 19.363, 'std_temp': 0.644,
        'avg_hum': 75.444, 'std_hum': 1.976,
        'avg_co': 0.003527, 'std_co': 0.001479,
        'avg_lpg': 0.005893, 'std_lpg': 0.001700,
        'avg_smoke': 0.015489, 'std_smoke': 0.004809,
        'pct_light': 0.032, 'pct_motion': 0.000,
        'breach_temp_range': (21.0, 26.0),
    },
    '1c:bf:ce:15:ec:4d': {
        'avg_temp': 26.026, 'std_temp': 2.026,
        'avg_hum': 61.910, 'std_hum': 8.945,
        'avg_co': 0.004183, 'std_co': 0.000320,
        'avg_lpg': 0.006764, 'std_lpg': 0.000373,
        'avg_smoke': 0.017895, 'std_smoke': 0.001055,
        'pct_light': 1.000, 'pct_motion': 0.003,
        'breach_temp_range': (30.0, 38.0),
    },
}

ROWS_PER_DEVICE_PER_DAY = 100
BREACH_RATE = 0.03        # 3% hari punya temperature breach episode
BREACH_READINGS_PCT = 0.08  # 8% readings dalam hari breach yang anomalous
TZ_OFFSET = timezone(timedelta(hours=7))

# ─── Date range untuk CSV (sebelum +5Y+10M transform di silver) ───────────────
# Target DB: 2021-01-01 s/d 2026-05-11
# Dikurangi 5Y 10M: 2015-03-01 s/d 2020-07-11
CSV_START = datetime(2015, 3, 1, tzinfo=TZ_OFFSET)
CSV_END   = datetime(2020, 7, 11, 23, 59, 59, tzinfo=TZ_OFFSET)

def generate_day_readings(date: datetime, device_mac: str, profile: dict) -> list:
    rows = []
    is_breach_day = np.random.random() < BREACH_RATE

    for i in range(ROWS_PER_DEVICE_PER_DAY):
        second_offset = int(np.random.uniform(0, 86399))
        ts = date + timedelta(seconds=second_offset)
        ts_str = ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + '+07'

        # Tentukan apakah reading ini adalah anomali temperature
        is_anomaly_reading = is_breach_day and (np.random.random() < BREACH_READINGS_PCT)

        if is_anomaly_reading:
            lo, hi = profile['breach_temp_range']
            temp = round(np.random.uniform(lo, hi), 1)
        else:
            temp = np.random.normal(profile['avg_temp'], profile['std_temp'])
            temp = round(float(np.clip(temp, -5, 45)), 1)

        hum = np.random.normal(profile['avg_hum'], profile['std_hum'])
        hum = round(float(np.clip(hum, 0, 100)), 6)

        co = abs(np.random.normal(profile['avg_co'], profile['std_co']))
        lpg = abs(np.random.normal(profile['avg_lpg'], profile['std_lpg']))
        smoke = abs(np.random.normal(profile['avg_smoke'], profile['std_smoke']))

        is_light = bool(np.random.random() < profile['pct_light'])
        is_motion = bool(np.random.random() < profile['pct_motion'])

        rows.append({
            'timestamp': ts_str,
            'device_mac': device_mac,
            'co_level': round(co, 6),
            'humidity_pct': round(hum, 6),
            'lpg_level': round(lpg, 6),
            'smoke_level': round(smoke, 6),
            'is_light': is_light,
            'is_motion': is_motion,
            'temp_celsius': temp,
        })
    return rows

# ─── Generate semua rows ──────────────────────────────────────────────────────
print("Generating synthetic cold chain data...")
print(f"Range CSV  : {CSV_START.date()} s/d {CSV_END.date()}")
print(f"Range DB   : 2021-01-01 s/d 2026-05-11 (setelah +5Y+10M transform)")
print(f"Rows/day   : {ROWS_PER_DEVICE_PER_DAY} × {len(DEVICES)} devices = {ROWS_PER_DEVICE_PER_DAY * len(DEVICES)}")

all_rows = []
current = CSV_START
day_count = 0

while current.date() <= CSV_END.date():
    for device_mac, profile in DEVICES.items():
        all_rows.extend(generate_day_readings(current, device_mac, profile))
    current += timedelta(days=1)
    day_count += 1
    if day_count % 365 == 0:
        print(f"  Progress: {day_count} hari selesai ({current.date()})...")

print(f"\nTotal synthetic rows generated: {len(all_rows):,}")

# ─── Buat DataFrame synthetic ─────────────────────────────────────────────────
df_synthetic = pd.DataFrame(all_rows)
df_synthetic = df_synthetic.sort_values('timestamp').reset_index(drop=True)

# ─── Ambil real data dari CSV yang sudah ada ──────────────────────────────────
print("\nMengambil real data dari data/iot_telemetry_cleaned.csv...")
REAL_CSV = "data/iot_telemetry_cleaned.csv"
df_real = pd.read_csv(REAL_CSV)
# Pastikan kolom sesuai dengan yang diharapkan
df_real = df_real.rename(columns={
    'device_mac': 'device_mac',
    'humidity_pct': 'humidity_pct',
    'temp_celsius': 'temp_celsius',
})
print(f"Real rows  : {len(df_real):,}")
print(f"Columns    : {df_real.columns.tolist()}")

# ─── Gabungkan dan simpan ke CSV ──────────────────────────────────────────────
df_combined = pd.concat([df_synthetic, df_real], ignore_index=True)
df_combined = df_combined.sort_values('timestamp').reset_index(drop=True)
print(f"Total combined: {len(df_combined):,} rows")

os.makedirs("data/raw", exist_ok=True)
output_path = "data/raw/iot_telemetry.csv"
df_combined.to_csv(output_path, index=False)
print(f"\nCSV saved  : {output_path} ({os.path.getsize(output_path) / 1024 / 1024:.1f} MB)")

# ─── Re-ingest ke bronze ──────────────────────────────────────────────────────
print("\nRe-ingesting ke bronze.iot_telemetry...")
DB_PATH = "data/warehouse.duckdb"
con_write = duckdb.connect(DB_PATH)
con_write.execute(f"""
    CREATE OR REPLACE TABLE bronze.iot_telemetry AS
    SELECT
        *,
        current_timestamp AS _loaded_at,
        '{output_path}' AS _source_file
    FROM read_csv_auto('{output_path}', timestampformat='%Y-%m-%d %H:%M:%S.%f+07');
""")
count = con_write.execute("SELECT COUNT(*) FROM bronze.iot_telemetry").fetchone()[0]
print(f"bronze.iot_telemetry: {count:,} rows")
con_write.close()

print("\nSelesai! Sekarang jalankan:")
print("  dbt run --select stg_iot_telemetry silver_fact_cold_chain gold_anomaly_check gold_mart_cold_chain_compliance gold_mart_executive_kpi")
