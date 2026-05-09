import duckdb
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

conn = duckdb.connect("data/warehouse.duckdb", read_only=True)

print("\n--- 1. VERIFIKASI DATE SHIFTING (Tahun 2026) ---")
query_dates = """
    SELECT 'Sales' as Table_Name, MAX(order_date_id) as Max_Date FROM silver.silver_fact_sales
    UNION ALL
    SELECT 'Inventory', MAX(date_id) FROM silver.silver_fact_inventory
    UNION ALL
    SELECT 'Cold Chain', MAX(date_id) FROM silver.silver_fact_cold_chain;
"""
print(conn.execute(query_dates).fetchdf())

print("\n--- 2. VERIFIKASI KATEGORI PRODUK ---")
query_category = """
    SELECT category_name, COUNT(*) as total_produk
    FROM silver.silver_dim_product
    GROUP BY category_name
    ORDER BY total_produk DESC;
"""
print(conn.execute(query_category).fetchdf())

print("\n--- 3. VERIFIKASI NAMA TOKO DESKRIPTIF ---")
query_store = "SELECT * FROM silver.silver_dim_store LIMIT 10;"
print(conn.execute(query_store).fetchdf())

conn.close()