import duckdb

# Sambungkan ke database Anda
# (Pastikan path ini sesuai dengan lokasi file database Anda)
conn = duckdb.connect("data/warehouse.duckdb") 

print("=== DAFTAR SKEMA ===")
# Menggunakan standar SQL (information_schema) alih-alih SHOW SCHEMAS
schemas = conn.execute("SELECT schema_name FROM information_schema.schemata;").fetchall()
for schema in schemas:
    print(f"- {schema[0]}")

# Skema bawaan (default) di DuckDB biasanya bernama 'main'
# Anda bisa menggantinya dengan 'bronze', 'silver', atau 'gold' jika sudah dibuat
schema_to_check = 'gold' 

print(f"\n=== DAFTAR TABEL DI SKEMA '{schema_to_check}' ===")
tables = conn.execute(f"""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = '{schema_to_check}';
""").fetchall()

if not tables:
    print("Tidak ada tabel di skema ini.")
else:
    for table in tables:
        print(f"\n- Tabel: {table[0]}")
        
        # Menampilkan 3 baris pertama dari tiap tabel menggunakan Pandas DataFrame
        print(f"  Isi data (3 baris pertama):")
        try:
            data = conn.execute(f"SELECT * FROM {schema_to_check}.{table[0]} LIMIT 3;").fetchdf()
            print(data)
        except Exception as e:
            print(f"  Gagal membaca data: {e}")

conn.close()