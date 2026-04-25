import duckdb

con = duckdb.connect('instacart_project.duckdb')

print("Membaca 5 baris pertama dari departments.csv...\n")

query_baca_csv = """
SELECT * FROM read_csv_auto('departments.csv') 
LIMIT 5;
"""

# Ganti .show() dengan .fetchall()
hasil_data = con.execute(query_baca_csv).fetchall()

# Menampilkan data baris demi baris ke layar
for baris in hasil_data:
    print(baris)