import os
import duckdb
from prefect import flow, task
from dotenv import load_dotenv

# Load jalur dari file .env
load_dotenv()
DB_PATH = os.getenv("DUCKDB_PATH", "data/database_eco_retail.duckdb")

@task
def test_koneksi_duckdb():
    # Menghubungkan ke DuckDB
    print(f"Menyambungkan ke database di: {DB_PATH}")
    conn = duckdb.connect(DB_PATH)
    
    # Membuat tabel percobaan sederhana
    conn.execute("CREATE TABLE IF NOT EXISTS test_table (id INTEGER, nama VARCHAR)")
    conn.execute("INSERT INTO test_table VALUES (1, 'Sistem OK')")
    
    # Menampilkan hasilnya
    hasil = conn.execute("SELECT * FROM test_table").fetchall()
    print("Berhasil! Isi database:", hasil)
    conn.close()

@flow(name="Pipeline Utama Eco-Retail")
def jalankan_pipeline():
    print("Memulai mandor Prefect...")
    test_koneksi_duckdb()

if __name__ == "__main__":
    jalankan_pipeline()