import duckdb
import pandas as pd

try:
    conn = duckdb.connect('data/warehouse.duckdb', read_only=True)
    
    queries = {
        "Food Waste Rate (%) & Total Loss Value": """
            SELECT 
                SUM(unsold_qty) * 100.0 / NULLIF(SUM(total_stock), 0) AS avg_food_waste_rate_pct,
                SUM(potential_waste_value) AS total_loss_value_rp
            FROM gold.gold_mart_food_waste_summary
        """,
        "Cold Chain Compliance Rate (%)": """
            SELECT AVG(compliance_rate_pct) AS avg_compliance_rate_pct
            FROM gold.gold_mart_cold_chain_compliance
        """,
        "Total Revenue": """
            SELECT SUM(total_revenue) AS total_revenue
            FROM gold.gold_mart_executive_kpi
        """,
        "Demand Forecast (Unit) - Next 7 Days": """
            SELECT SUM(yhat) AS total_forecast_units
            FROM gold.ml_demand_predictions
        """,
        "Trend Food Waste Rate": """
            SELECT 
                date_id, 
                SUM(unsold_qty) * 100.0 / NULLIF(SUM(total_stock), 0) AS waste_rate_pct
            FROM gold.gold_mart_food_waste_summary
            GROUP BY date_id
            ORDER BY date_id
            LIMIT 5
        """,
        "Waste by Category": """
            SELECT 
                category_name, 
                SUM(unsold_qty) AS total_wasted_qty
            FROM gold.gold_mart_food_waste_summary
            GROUP BY category_name
            ORDER BY total_wasted_qty DESC
            LIMIT 5
        """,
        "Cold Chain Breach Timeline": """
            SELECT 
                date_id, 
                SUM(equipment_breach_count) AS total_breaches
            FROM gold.gold_mart_cold_chain_compliance
            WHERE equipment_breach_count > 0
            GROUP BY date_id
            ORDER BY date_id
            LIMIT 5
        """,
        "Top 5 Locations dengan Waste Tertinggi": """
            SELECT 
                store_id, 
                SUM(unsold_qty) AS total_waste_qty
            FROM gold.gold_mart_food_waste_summary
            GROUP BY store_id
            ORDER BY total_waste_qty DESC
            LIMIT 5
        """
    }

    for name, q in queries.items():
        print(f"--- {name} ---")
        try:
            print(conn.execute(q).fetchdf())
        except Exception as q_e:
            print(f"Query Error: {q_e}")
    
    conn.close()
except Exception as e:
    print(f'Error: {e}')
