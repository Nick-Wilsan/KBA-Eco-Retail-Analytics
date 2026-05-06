import subprocess
from prefect import flow, task

@task(name="1. Jalankan Silver Layer (Data Transformation)", retries=1)
def run_dbt_silver():
    """Memicu dbt untuk memperbarui data pembersihan dan agregasi."""
    print("Menjalankan perintah: dbt run --select silver")
    
    # Menjalankan perintah dbt lewat shell/terminal Python
    result = subprocess.run(["dbt", "run", "--select", "silver"], capture_output=True, text=True)
    print(result.stdout)
    
    if result.returncode != 0:
        print(result.stderr)
        raise Exception("Gagal menjalankan dbt silver layer!")

@task(name="2. Jalankan Gold Layer & Machine Learning", retries=1)
def run_dbt_gold():
    """Memicu dbt untuk menjalankan model SQL dan Python (Prophet & Isolation Forest)."""
    print("Menjalankan perintah: dbt run --select gold")
    
    # Saat ini dijalankan, dbt akan otomatis mengeksekusi file .py buatan Kresna!
    result = subprocess.run(["dbt", "run", "--select", "gold"], capture_output=True, text=True)
    print(result.stdout)
    
    if result.returncode != 0:
        print(result.stderr)
        raise Exception("Gagal menjalankan dbt gold layer dan ML model!")

@flow(name="Weekly Pipeline - ML and Data Warehouse")
def weekly_dbt_pipeline():
    """Orkestrasi utama untuk menyegarkan Data Warehouse dan Prediksi AI"""
    print("=== Memulai Pipeline Mingguan ===")
    run_dbt_silver()
    run_dbt_gold()
    print("=== Pipeline Selesai ===")

if __name__ == "__main__":
    # Ini akan membuat Prefect standby dan menjalankan dbt otomatis tiap minggu
    # Jadwal CRON: "0 2 * * 1" berarti Menit 0, Jam 02:00 Pagi, Setiap Hari Senin
    
    # Gunakan baris ini untuk testing manual:
    # weekly_dbt_pipeline()
    
    # Gunakan baris ini untuk mode jadwal otomatis (Server):
    weekly_dbt_pipeline.serve(
        name="weekly-ml-dbt-run",
        cron="0 2 * * 1",
        tags=["dbt", "machine-learning", "duckdb"]
    )