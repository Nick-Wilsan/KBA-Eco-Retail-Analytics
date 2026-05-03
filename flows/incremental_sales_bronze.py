import os
import duckdb
from prefect import flow, task
from dotenv import load_dotenv

# Load konfigurasi environment
load_dotenv()
DB_PATH = os.getenv("DUCKDB_PATH", "data/warehouse.duckdb")

# Konfigurasi Dataset
CSV_PATH = "data/raw/retail_data.csv"
SCHEMA_NAME = "bronze"  # Sesuaikan jika Anda masih menggunakan 'warehouse_bronze'
TABLE_NAME = "retail_data"

@task(name="1. Dapatkan Watermark Terakhir", retries=2)
def get_max_watermark():
    """
    Mencari order_item_id tertinggi di tabel yang sudah ada.
    Jika tabel belum ada, kembalikan None (akan trigger Full Load).
    """
    print(f"Memeriksa watermark di {SCHEMA_NAME}.{TABLE_NAME}...")
    conn = duckdb.connect(DB_PATH)
    
    try:
        # Cek apakah tabel sudah eksis di DuckDB
        table_check_query = f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = '{SCHEMA_NAME}' AND table_name = '{TABLE_NAME}'
        """
        table_exists = conn.execute(table_check_query).fetchone()[0] > 0
        
        if not table_exists:
            print("Tabel belum ada. Bersiap melakukan Full Load/Initial Load.")
            return None
        
        # Ambil nilai maksimum dari order_item_id
        max_val = conn.execute(f"SELECT MAX(order_item_id) FROM {SCHEMA_NAME}.{TABLE_NAME}").fetchone()[0]
        print(f"Watermark ditemukan. ID transaksi terakhir: {max_val}")
        return max_val
        
    except Exception as e:
        print(f"Gagal mendapatkan watermark: {e}")
        raise e
    finally:
        conn.close()

@task(name="2. Eksekusi Incremental Ingest", retries=2)
def incremental_ingest(max_watermark):
    """
    Melakukan ingest data dari CSV. Full load jika max_watermark None.
    Jika ada, filter hanya data yang lebih baru.
    """
    conn = duckdb.connect(DB_PATH)
    
    try:
        if max_watermark is None:
            # Skenario 1: Tabel belum ada, muat semua data
            query_full_load = f"""
                CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};
                CREATE OR REPLACE TABLE {SCHEMA_NAME}.{TABLE_NAME} AS 
                SELECT 
                    *, 
                    current_timestamp AS _loaded_at, 
                    '{CSV_PATH}' AS _source_file
                FROM read_csv_auto('{CSV_PATH}');
            """
            conn.execute(query_full_load)
            print("Status: Full Load Selesai.")
            
        else:
            # Skenario 2: Tabel sudah ada, muat data baru (Incremental)
            query_incremental = f"""
                INSERT INTO {SCHEMA_NAME}.{TABLE_NAME}
                SELECT 
                    *, 
                    current_timestamp AS _loaded_at, 
                    '{CSV_PATH}' AS _source_file
                FROM read_csv_auto('{CSV_PATH}')
                WHERE order_item_id > {max_watermark};
            """
            conn.execute(query_incremental)
            print("Status: Incremental Load Selesai.")

        # Hitung total baris saat ini untuk verifikasi
        total_rows = conn.execute(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.{TABLE_NAME}").fetchone()[0]
        print(f"Total baris di tabel {SCHEMA_NAME}.{TABLE_NAME} saat ini: {total_rows}")
        
    except Exception as e:
        print(f"Gagal melakukan ingest: {e}")
        raise e
    finally:
        conn.close()

@flow(name="Bronze Incremental Flow - Retail Sales")
def flow_incremental_sales():
    """
    Orkestrasi utama Prefect untuk memuat data penjualan ritel secara inkremental.
    """
    print("=== Memulai Pipeline Incremental Retail Sales ===")
    
    # Task 1: Ambil ID terakhir
    watermark = get_max_watermark()
    
    # Task 2: Tarik data baru berdasarkan ID terakhir
    incremental_ingest(watermark)
    
    print("=== Pipeline Selesai ===")

if __name__ == "__main__":
    flow_incremental_sales()
    