import os
import duckdb
from dotenv import load_dotenv

load_dotenv()
# Pastikan path ini sinkron dengan konfigurasi dbt Anda (data/warehouse.duckdb)
DB_PATH = os.getenv("DUCKDB_PATH", "data/warehouse.duckdb")

def setup_medallion_schemas():
    print(f"Menyambungkan ke database di: {DB_PATH}")
    conn = duckdb.connect(DB_PATH)
    
    try:
        # Membuat skema arsitektur Medallion
        print("Membangun skema Medallion...")
        conn.execute("CREATE SCHEMA IF NOT EXISTS bronze;") # Raw Data
        conn.execute("CREATE SCHEMA IF NOT EXISTS silver;") # Cleaned/Conformed Data
        conn.execute("CREATE SCHEMA IF NOT EXISTS gold;")   # Analytics-ready Data
        
        # Verifikasi skema yang berhasil dibuat
        schemas = conn.execute("SELECT schema_name FROM information_schema.schemata;").fetchall()
        print(f"Skema yang tersedia saat ini: {[s[0] for s in schemas]}")
        
    except Exception as e:
        print(f"Gagal membuat skema: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    setup_medallion_schemas()