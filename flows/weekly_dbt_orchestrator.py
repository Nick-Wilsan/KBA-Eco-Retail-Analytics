
import os
import subprocess
import shutil
from prefect import flow, task

def _find_dbt():
    """Cari executable dbt: cek PATH, lalu fallback ke lokasi user scripts Windows."""
    dbt = shutil.which("dbt")
    if dbt:
        return dbt
    # Fallback untuk Windows Store Python yang menaruh scripts di AppData
    scripts_dir = os.path.join(
        os.environ.get("LOCALAPPDATA", ""),
        "Packages",
        "PythonSoftwareFoundation.Python.3.13_qbz5n2kfra8p0",
        "LocalCache", "local-packages", "Python313", "Scripts", "dbt.exe"
    )
    if os.path.exists(scripts_dir):
        return scripts_dir
    raise FileNotFoundError("dbt executable tidak ditemukan. Pastikan dbt-core sudah terinstall.")

DBT_EXE = _find_dbt()

@task(name="1. Jalankan Silver Layer (Data Transformation)", retries=1)
def run_dbt_silver():
    """Memicu dbt untuk memperbarui data pembersihan dan agregasi."""
    print(f"Menjalankan: {DBT_EXE} run --select silver")

    result = subprocess.run(
        [DBT_EXE, "run", "--select", "silver"],
        capture_output=True, text=True
    )
    print(result.stdout)

    if result.returncode != 0:
        print(result.stderr)
        raise Exception("Gagal menjalankan dbt silver layer!")

@task(name="2. Jalankan Gold Layer & Machine Learning", retries=1)
def run_dbt_gold():
    """Memicu dbt untuk menjalankan model SQL dan Python (Prophet & Isolation Forest)."""
    print(f"Menjalankan: {DBT_EXE} run --select gold")

    result = subprocess.run(
        [DBT_EXE, "run", "--select", "gold"],
        capture_output=True, text=True
    )
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
    weekly_dbt_pipeline()
    
    # Gunakan baris ini untuk mode jadwal otomatis (Server):
    # weekly_dbt_pipeline.serve(
    #     name="weekly-ml-dbt-run",
    #     cron="0 2 * * 1",
    #     tags=["dbt", "machine-learning", "duckdb"]
    # )