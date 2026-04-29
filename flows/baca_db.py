import duckdb

# Jalur menuju database Anda
DB_PATH = "data/database_eco_retail.duckdb"

print("Membuka database...")
conn = duckdb.connect(DB_PATH)

# Menampilkan daftar semua tabel yang ada di database
print("\n--- Daftar Tabel ---")
tabel = conn.execute("SHOW TABLES").fetchall()
for t in tabel:
    print(f"- {t[0]}")

# Menampilkan isi dari test_table
print("\n--- Isi test_table ---")
isi_data = conn.execute("SELECT * FROM test_table").fetchall()
for baris in isi_data:
    print(baris)

conn.close()