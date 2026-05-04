import duckdb

# Hubungkan ke Data Warehouse Anda (read_only=True agar aman)
conn = duckdb.connect("data/warehouse.duckdb", read_only=True)

# 1. Kueri yang ingin kita tes (Misal: Metabase mencari data waste di toko tertentu)
test_query = """
    SELECT * 
    FROM warehouse_gold.gold_mart_food_waste_summary 
    WHERE store_id = 'STR-001'
"""

print("="*70)
print("🔍 EXPLAIN (Rencana Eksekusi)")
print("="*70)
# Tambahkan kata EXPLAIN di depan kueri
explain_result = conn.execute(f"EXPLAIN {test_query}").fetchall()
for row in explain_result:
    print(row[1]) # row[1] berisi grafis pohon eksekusi dari DuckDB

print("\n" + "="*70)
print("⏱️ EXPLAIN ANALYZE (Profil Eksekusi Aktual)")
print("="*70)
# Tambahkan kata EXPLAIN ANALYZE di depan kueri
explain_analyze_result = conn.execute(f"EXPLAIN ANALYZE {test_query}").fetchall()
for row in explain_analyze_result:
    print(row[1])

conn.close()