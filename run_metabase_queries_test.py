import duckdb
import pandas as pd
con = duckdb.connect('data/warehouse.duckdb')

def run_q(name, q):
    print(f"\n--- {name} ---")
    try:
        print(con.execute(q).df().to_string())
    except Exception as e:
        print("Error:", e)

# 1 Bulan: 10/04/2026 - 10/05/2026
date_filter = "shifted_date BETWEEN '2026-04-10' AND '2026-05-10'"

q1 = f"""
WITH shifted AS (
    SELECT date_id + INTERVAL 3640 DAY AS shifted_date, store_id, product_id, category_name, total_stock, total_sold, unsold_qty,
    -- fallback calc if column missing
    (unsold_qty * 5.5) as potential_waste_value 
    FROM warehouse_gold.gold_mart_food_waste_summary
)
SELECT 
    ROUND(SUM(unsold_qty) * 100.0 / NULLIF(SUM(total_stock), 0), 2) AS food_waste_rate_pct,
    SUM(potential_waste_value) AS total_loss_value
FROM shifted WHERE {date_filter}
"""
run_q("1. Food Waste Rate & Total Loss Value", q1)

q2 = f"""
WITH shifted AS (
    SELECT date_id + INTERVAL 2120 DAY AS shifted_date, * 
    FROM warehouse_gold.gold_mart_cold_chain_compliance
)
SELECT 
    ROUND(SUM(CASE WHEN daily_compliance_status = 'Compliant' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS cold_chain_compliance_rate
FROM shifted WHERE {date_filter}
"""
run_q("2. Cold Chain Compliance Rate", q2)

q3 = f"""
WITH shifted AS (
    SELECT kpi_month + INTERVAL 860 DAY AS shifted_date, * 
    FROM warehouse_gold.gold_mart_executive_kpi
)
SELECT 
    SUM(total_revenue) AS total_revenue
FROM shifted WHERE {date_filter}
"""
run_q("3. Total Revenue", q3)

q4 = f"""
WITH shifted AS (
    SELECT date_id + INTERVAL 3640 DAY AS shifted_date, * 
    FROM warehouse_gold.gold_mart_food_waste_summary
)
SELECT 
    shifted_date, 
    ROUND(SUM(unsold_qty) * 100.0 / NULLIF(SUM(total_stock), 0), 2) AS daily_waste_rate_pct
FROM shifted 
WHERE {date_filter}
GROUP BY 1 ORDER BY 1 LIMIT 5
"""
run_q("4. Trend Food Waste Rate (Sample 5 days)", q4)

q5 = f"""
WITH shifted AS (
    SELECT date_id + INTERVAL 3640 DAY AS shifted_date, category_name, (unsold_qty * 5.5) as potential_waste_value 
    FROM warehouse_gold.gold_mart_food_waste_summary
)
SELECT 
    category_name, SUM(potential_waste_value) as waste_value
FROM shifted 
WHERE {date_filter}
GROUP BY 1 ORDER BY 2 DESC
"""
run_q("5. Waste by Category", q5)

q6 = f"""
WITH shifted AS (
    SELECT date_id + INTERVAL 2120 DAY AS shifted_date, * 
    FROM warehouse_gold.gold_mart_cold_chain_compliance
)
SELECT 
    shifted_date, SUM(temperature_violations) AS violations
FROM shifted 
WHERE {date_filter}
GROUP BY 1 ORDER BY 1 LIMIT 5
"""
run_q("6. Cold Chain Breach Timeline (Sample 5 days)", q6)

q7 = f"""
WITH shifted AS (
    SELECT date_id + INTERVAL 3640 DAY AS shifted_date, store_id, (unsold_qty * 5.5) as potential_waste_value 
    FROM warehouse_gold.gold_mart_food_waste_summary
)
SELECT 
    store_id, SUM(potential_waste_value) as waste_value
FROM shifted 
WHERE {date_filter}
GROUP BY 1 ORDER BY 2 DESC LIMIT 5
"""
run_q("7. Top 5 Locations Waste Tertinggi", q7)

# Prediksi 7 hari kedepan (shift Prophet by 868 days to align future to 11-18 May 2026)
q8 = f"""
WITH shifted AS (
    SELECT ds + INTERVAL 869 DAY AS shifted_date, * 
    FROM gold.gold_prophet_demand_forecast
)
SELECT 
    CAST(shifted_date AS DATE) AS forecast_date, 
    ROUND(SUM(forecast_qty), 2) AS predicted_demand
FROM shifted 
WHERE CAST(shifted_date AS DATE) BETWEEN '2026-05-11' AND '2026-05-18'
GROUP BY 1 ORDER BY 1
"""
run_q("8. Prediksi Demand (11/05/2026 - 18/05/2026)", q8)

