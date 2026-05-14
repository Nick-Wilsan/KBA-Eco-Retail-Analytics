import duckdb

conn = duckdb.connect('data/warehouse.duckdb')
res = conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'gold_mart_food_waste_summary'").fetchall()
print(res)
conn.close()