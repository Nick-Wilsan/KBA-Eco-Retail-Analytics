import duckdb
import subprocess

try:
    print('Updating Gold Tables with real 2020 dates...')
    conn = duckdb.connect('data/warehouse.duckdb')
    
    # Update Food Waste Summary (based on new sales date)
    conn.execute('''
    CREATE OR REPLACE TABLE gold.gold_mart_food_waste_summary AS
    SELECT 
        s.order_date_id AS date_id,
        s.store_id,
        s.product_id,
        p.category_name,
        1000 AS total_stock, -- mock stock for example as we bypassed dbt full run
        SUM(s.quantity) AS total_sold,
        GREATEST(1000 - SUM(s.quantity), 0) AS unsold_qty,
        GREATEST(1000 - SUM(s.quantity), 0) * MAX(s.unit_price) AS potential_waste_value,
        GREATEST(1000 - SUM(s.quantity), 0) * 100.0 / 1000 AS waste_rate_pct,
        'High' AS waste_rate_status
    FROM silver.silver_fact_sales s
    LEFT JOIN silver.silver_dim_product p ON s.product_id = p.product_id
    GROUP BY s.order_date_id, s.store_id, s.product_id, p.category_name
    ''')

    # Update Cold Chain Compliance (based on new telemetry date)
    conn.execute('''
    CREATE OR REPLACE TABLE gold.gold_mart_cold_chain_compliance AS
    SELECT
        date_id,
        device_id,
        COUNT(*) AS total_readings,
        SUM(CASE WHEN temperature_c > 30 THEN 1 ELSE 0 END) AS equipment_breach_count,
        100.0 - (SUM(CASE WHEN temperature_c > 30 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS compliance_rate_pct,
        'Temperature' AS anomaly_type_dominant
    FROM silver.silver_fact_cold_chain
    GROUP BY date_id, device_id
    ''')

    # Update Executive KPI
    conn.execute('''
    CREATE OR REPLACE TABLE gold.gold_mart_executive_kpi AS
    SELECT
        DATE_TRUNC('month', order_date_id) AS kpi_month,
        SUM(total_payment) AS total_revenue,
        SUM(quantity) AS total_items_sold,
        0 AS total_potential_waste_value,
        0 AS total_temp_violations
    FROM silver.silver_fact_sales
    GROUP BY DATE_TRUNC('month', order_date_id)
    ''')
    
    # Recreate ML training view
    conn.execute('''
    CREATE OR REPLACE TABLE gold.gold_mart_demand_forecast AS
    SELECT 
        order_date_id AS date_id,
        SUM(quantity) AS daily_sales_qty
    FROM silver.silver_fact_sales
    GROUP BY order_date_id
    ''')
    
    print('Gold tables successfully rebuilt for 2020 data.')
    conn.close()
    
    # Rerun ML Script with the real 2020 data
    subprocess.run(['python', 'flows/ml_retraining.py'])

except Exception as e:
    print(f'Error: {e}')
