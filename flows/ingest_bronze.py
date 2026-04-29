import os
import glob
import duckdb
from prefect import flow, task
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
DB_PATH = os.getenv("DUCKDB_PATH", "data/warehouse.duckdb")

# Menentukan folder tempat dataset CSV Kaggle Anda disimpan

RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", "data/raw/")

@task(name="Ingest CSV ke Bronze", retries=2)
def ingest_csv_to_bronze(csv_path: str, table_name: str):
    """
    Tugas memuat satu file CSV ke dalam skema bronze DuckDB 
    beserta tambahan metadata dasar.
    """
    print(f"▶ Memproses: {csv_path} -> Skema: bronze.{table_name}")
    
    conn = duckdb.connect(DB_PATH)
    try:
        # 1. Ingest data menggunakan read_csv_auto 
        # CREATE OR REPLACE memastikan pipeline aman dijalankan ulang (idempotent)
        query = f"""
            CREATE OR REPLACE TABLE bronze.{table_name} AS 
            SELECT 
                *, 
                current_timestamp AS _loaded_at, 
                '{csv_path}' AS _source_file
            FROM read_csv_auto('{csv_path}');
        """
        conn.execute(query)
        
        # 2. Verifikasi hasilnya
        count = conn.execute(f"SELECT COUNT(*) FROM bronze.{table_name}").fetchone()[0]
        print(f"✔ Berhasil memuat {count} baris ke tabel bronze.{table_name}")
        
    except Exception as e:
        print(f"✖ Error saat ingest {csv_path}: {e}")
        raise e
    finally:
        conn.close()

@flow(name="Bronze Layer Ingestion Flow")
def flow_ingest_bronze():
    """
    Prefect Flow utama yang akan mencari semua file CSV dan mengorkestrasinya
    """
    print(f"Mencari dataset CSV di direktori: {RAW_DATA_DIR}")
    
    # Mencari semua file dengan ekstensi .csv di direktori yang ditentukan
    csv_files = glob.glob(os.path.join(RAW_DATA_DIR, "*.csv"))
    
    if not csv_files:
        print(f"Peringatan: Tidak ada file CSV ditemukan di {RAW_DATA_DIR}!")
        return

    # Looping semua file yang ditemukan dan jalankan task Prefect
    for csv_path in csv_files:
        # Ambil nama file tanpa ekstensi untuk dijadikan nama tabel
        # Contoh: data/raw/transactions.csv -> transactions
        file_name = os.path.basename(csv_path)
        table_name = os.path.splitext(file_name)[0]
        
        # Panggil Prefect Task
        ingest_csv_to_bronze(csv_path, table_name)
    
    print("Pipeline Bronze selesai dieksekusi!")

if __name__ == "__main__":
    flow_ingest_bronze()