import duckdb
import pandas as pd

# Setting Pandas agar menampilkan semua kolom saat di-print ke terminal
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

# Ingat gunakan read_only=True agar tidak error terkunci
conn = duckdb.connect("data/warehouse.duckdb", read_only=True)

print("="*60)
print("VOLUME DATA DI BRONZE")
print("="*60)
query_bronze = "SELECT COUNT(*) AS total_rows FROM bronze.instacart_grocery;"
print(f"Query: {query_bronze}\n")
print(conn.execute(query_bronze).fetchdf())
print("\n")

print("="*60)
print("CONTOH TABEL GOLD (WASTE RATE PCT & STATUS)")
print("="*60)
query_gold = "SELECT * FROM warehouse_gold.gold_mart_food_waste_summary LIMIT 5;"
print(f"Query: {query_gold}\n")
print(conn.execute(query_gold).fetchdf())

conn.close()