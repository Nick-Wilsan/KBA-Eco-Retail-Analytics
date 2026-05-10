import duckdb
import pandas as pd

con = duckdb.connect('data/warehouse.duckdb')

print('=== DATE RANGES ===')
r = con.execute("SELECT MIN(date_id), MAX(date_id) FROM gold.gold_mart_food_waste_summary").fetchone()
print(f'food_waste_summary: {r}')

r = con.execute("SELECT MIN(date_id), MAX(date_id) FROM gold.gold_mart_cold_chain_compliance").fetchone()
print(f'cold_chain_compliance: {r}')

r = con.execute("SELECT MIN(kpi_month), MAX(kpi_month) FROM gold.gold_mart_executive_kpi").fetchone()
print(f'executive_kpi: {r}')

r = con.execute("SELECT MIN(ds), MAX(ds) FROM gold.gold_mart_demand_forecast").fetchone()
print(f'demand_forecast: {r}')

r = con.execute("SELECT MIN(ds), MAX(ds) FROM gold.gold_prophet_demand_forecast").fetchone()
print(f'prophet_demand_forecast: {r}')
