# Eco-Retail ERP Analytics

Proyek Business Intelligence (BI) untuk optimisasi Food Waste dan Rantai Pasok Dingin (Cold Chain) menggunakan ekosistem open-source.

## 🏗️ Architecture & Tech Stack

Proyek ini mengadopsi **Medallion Architecture** (Bronze, Silver, Gold) untuk memastikan kualitas dan skalabilitas data. Seluruh infrastruktur di-deploy secara lokal menggunakan **Docker Compose**.

* **Orchestration:** Prefect (Menggantikan Airflow untuk otomasi pipeline yang lebih ringan dan python-native).
* **Data Warehouse / Database:** DuckDB (OLAP database berkinerja tinggi).
* **Data Transformation:** dbt (data build tool) untuk pembersihan dan agregasi data antar layer Medallion.
* **Business Intelligence:** Metabase (Dashboarding dengan Role-Based Access Control / RBAC).
* **Machine Learning:** Scikit-learn & Prophet (Dalam pengembangan untuk fase analitik prediktif).

## Cara Menjalankan Sistem
1. Pastikan Docker Desktop sudah terinstal dan berjalan.
2. Clone repositori ini.
3. Buka terminal di dalam folder proyek, lalu jalankan:
   `docker-compose up -d`
4. Akses layanan:
   - Airflow: `http://localhost:8080` (Username/Password akan muncul di terminal)
   - Metabase: `http://localhost:3000`