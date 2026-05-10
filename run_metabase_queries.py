import duckdb
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)
pd.set_option('display.float_format', '{:,.2f}'.format)

con = duckdb.connect('data/warehouse.duckdb')

# ============================================================
# BAGIAN 1: KPI SUMMARY — 3 RENTANG WAKTU
# ============================================================

for label, start, end, kpi_start, kpi_end in [
    ("1 TAHUN (2025-05-10 s/d 2026-05-10)", "2025-05-10", "2026-05-10", "2025-05-01", "2026-05-01"),
    ("1 BULAN (2026-04-10 s/d 2026-05-10)", "2026-04-10", "2026-05-10", "2026-04-01", "2026-05-01"),
    ("1 MINGGU (2026-05-03 s/d 2026-05-10)", "2026-05-03", "2026-05-10", "2026-05-01", "2026-05-01"),
]:
    print(f"\n{'='*70}\nKPI SUMMARY — {label}\n{'='*70}")
    df = con.execute(f"""
        SELECT
            ROUND(SUM(unsold_qty) * 100.0 / NULLIF(SUM(total_stock), 0), 2) AS food_waste_rate_pct,
            ROUND(SUM(potential_waste_value), 2) AS total_loss_value
        FROM gold.gold_mart_food_waste_summary
        WHERE date_id BETWEEN '{start}' AND '{end}'
    """).df()
    print(df.to_string(index=False))

    df2 = con.execute(f"""
        SELECT
            ROUND(AVG(compliance_rate_pct), 2) AS cold_chain_compliance_rate_pct,
            SUM(equipment_breach_count) AS total_breaches
        FROM gold.gold_mart_cold_chain_compliance
        WHERE date_id BETWEEN '{start}' AND '{end}'
    """).df()
    print(df2.to_string(index=False))

    df3 = con.execute(f"""
        SELECT ROUND(SUM(total_revenue), 2) AS total_revenue
        FROM gold.gold_mart_executive_kpi
        WHERE kpi_month BETWEEN '{kpi_start}' AND '{kpi_end}'
    """).df()
    print(df3.to_string(index=False))

# ============================================================
# BAGIAN 2: TREND FOOD WASTE RATE
# ============================================================

print("\n" + "="*70)
print("TREND FOOD WASTE RATE — 1 TAHUN (per bulan)")
print("="*70)
df = con.execute("""
    SELECT
        DATE_TRUNC('month', date_id) AS bulan,
        ROUND(SUM(unsold_qty) * 100.0 / NULLIF(SUM(total_stock), 0), 2) AS food_waste_rate_pct,
        ROUND(SUM(potential_waste_value), 2) AS total_loss_value
    FROM gold.gold_mart_food_waste_summary
    WHERE date_id BETWEEN '2025-05-10' AND '2026-05-10'
    GROUP BY 1 ORDER BY 1
""").df()
print(df.to_string(index=False))

print("\n" + "="*70)
print("TREND FOOD WASTE RATE — 1 BULAN (per hari)")
print("="*70)
df = con.execute("""
    SELECT
        date_id,
        ROUND(SUM(unsold_qty) * 100.0 / NULLIF(SUM(total_stock), 0), 2) AS food_waste_rate_pct,
        ROUND(SUM(potential_waste_value), 2) AS total_loss_value
    FROM gold.gold_mart_food_waste_summary
    WHERE date_id BETWEEN '2026-04-10' AND '2026-05-10'
    GROUP BY date_id ORDER BY date_id
""").df()
print(df.to_string(index=False))

print("\n" + "="*70)
print("TREND FOOD WASTE RATE — 1 MINGGU (per hari)")
print("="*70)
df = con.execute("""
    SELECT
        date_id,
        ROUND(SUM(unsold_qty) * 100.0 / NULLIF(SUM(total_stock), 0), 2) AS food_waste_rate_pct,
        ROUND(SUM(potential_waste_value), 2) AS total_loss_value
    FROM gold.gold_mart_food_waste_summary
    WHERE date_id BETWEEN '2026-05-03' AND '2026-05-10'
    GROUP BY date_id ORDER BY date_id
""").df()
print(df.to_string(index=False))

# ============================================================
# BAGIAN 3: WASTE BY CATEGORY (extract dari M5 product naming)
# ============================================================

for label, start, end in [
    ("1 TAHUN (2025-05-10 s/d 2026-05-10)", "2025-05-10", "2026-05-10"),
    ("1 BULAN (2026-04-10 s/d 2026-05-10)", "2026-04-10", "2026-05-10"),
    ("1 MINGGU (2026-05-03 s/d 2026-05-10)", "2026-05-03", "2026-05-10"),
]:
    print(f"\n{'='*70}\nWASTE BY CATEGORY — {label}\n{'='*70}")
    df = con.execute(f"""
        SELECT
            CASE
                WHEN f.product_id LIKE 'FOODS%'     THEN 'Foods'
                WHEN f.product_id LIKE 'HOUSEHOLD%' THEN 'Household'
                WHEN f.product_id LIKE 'HOBBIES%'   THEN 'Hobbies'
                ELSE COALESCE(p.category_name, 'Other')
            END AS category,
            ROUND(SUM(potential_waste_value), 2) AS total_loss_value,
            ROUND(SUM(unsold_qty) * 100.0 / NULLIF(SUM(total_stock), 0), 2) AS waste_rate_pct,
            SUM(unsold_qty) AS total_unsold_qty
        FROM gold.gold_mart_food_waste_summary f
        LEFT JOIN silver.silver_dim_product p ON f.product_id = p.product_id
        WHERE f.date_id BETWEEN '{start}' AND '{end}'
        GROUP BY 1
        ORDER BY total_loss_value DESC
    """).df()
    print(df.to_string(index=False))

# ============================================================
# BAGIAN 4: COLD CHAIN BREACH TIMELINE
# ============================================================

print("\n" + "="*70)
print("COLD CHAIN BREACH TIMELINE — DATA TERSEDIA (2026-05-12 s/d 2026-05-20)")
print("="*70)
df = con.execute("""
    SELECT
        date_id,
        SUM(equipment_breach_count) AS total_breaches,
        ROUND(AVG(compliance_rate_pct), 2) AS avg_compliance_rate_pct,
        COUNT(DISTINCT device_id) AS devices_monitored
    FROM gold.gold_mart_cold_chain_compliance
    GROUP BY date_id
    ORDER BY date_id
""").df()
print(df.to_string(index=False))

print("\n[INFO] Cold chain data hanya tersedia dari 2026-05-12 s/d 2026-05-20.")
print("Data IoT baru mulai dikumpulkan sejak pertengahan Mei 2026.")

# ============================================================
# BAGIAN 5: TOP 5 LOCATIONS BY WASTE
# ============================================================

for label, start, end in [
    ("1 TAHUN (2025-05-10 s/d 2026-05-10)", "2025-05-10", "2026-05-10"),
    ("1 BULAN (2026-04-10 s/d 2026-05-10)", "2026-04-10", "2026-05-10"),
    ("1 MINGGU (2026-05-03 s/d 2026-05-10)", "2026-05-03", "2026-05-10"),
]:
    print(f"\n{'='*70}\nTOP 5 LOCATIONS BY WASTE — {label}\n{'='*70}")
    df = con.execute(f"""
        SELECT
            f.store_id,
            COALESCE(s.store_name, 'Store ' || f.store_id) AS store_name,
            COALESCE(s.city, s.state, 'Unknown') AS location,
            ROUND(SUM(f.potential_waste_value), 2) AS total_loss_value,
            ROUND(SUM(f.unsold_qty) * 100.0 / NULLIF(SUM(f.total_stock), 0), 2) AS waste_rate_pct,
            SUM(f.unsold_qty) AS total_unsold_qty
        FROM gold.gold_mart_food_waste_summary f
        LEFT JOIN silver.silver_dim_store s ON f.store_id = s.store_id
        WHERE f.date_id BETWEEN '{start}' AND '{end}'
        GROUP BY f.store_id, s.store_name, s.city, s.state
        ORDER BY total_loss_value DESC
        LIMIT 5
    """).df()
    print(df.to_string(index=False))

# ============================================================
# BAGIAN 6: DEMAND FORECAST 7 HARI KE DEPAN (11/05 - 18/05/2026)
# ============================================================

print("\n" + "="*70)
print("DEMAND FORECAST — 7 HARI KE DEPAN (2026-05-11 s/d 2026-05-18)")
print("="*70)
df = con.execute("""
    SELECT
        CAST(ds AS DATE) AS tanggal,
        ROUND(forecast_qty, 0) AS forecast_unit,
        ROUND(lower_bound, 0) AS lower_bound,
        ROUND(upper_bound, 0) AS upper_bound
    FROM gold.gold_mart_demand_forecast
    WHERE CAST(ds AS DATE) BETWEEN '2026-05-11' AND '2026-05-18'
    ORDER BY ds
""").df()
print(df.to_string(index=False))
