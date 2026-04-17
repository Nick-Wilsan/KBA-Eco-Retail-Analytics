# Eco-Retail ERP Analytics

Proyek Business Intelligence (BI) untuk optimisasi Food Waste dan Rantai Pasok Dingin (Cold Chain) menggunakan ekosistem open-source.

## Arsitektur Sistem
Sistem berjalan sepenuhnya di mesin lokal menggunakan Docker Compose dengan teknologi berikut:
- **Data Source:** Dataset Kaggle (CSV)
- **Data Warehouse:** DuckDB
- **Orchestration:** Apache Airflow
- **Transformation:** dbt Core
- **Visualization:** Metabase Community
- **Machine Learning:** Prophet & Scikit-learn (Python)

## Cara Menjalankan Sistem
1. Pastikan Docker Desktop sudah terinstal dan berjalan.
2. Clone repositori ini.
3. Buka terminal di dalam folder proyek, lalu jalankan:
   `docker-compose up -d`
4. Akses layanan:
   - Airflow: `http://localhost:8080` (Username/Password akan muncul di terminal)
   - Metabase: `http://localhost:3000`