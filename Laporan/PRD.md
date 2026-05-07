Product Requirement Document 

# Eco-Retail ERP Analytics 

Dosen Pengampu Kecerdasan Bisnis dan Analitik - A 

Ir. Nanang Yudi Setiawan, ST., M.Kom. 

## Penyusun 

M.S. Roeney Palessy - NIM 235150407111045 

Nick Wilsan - NIM 245150400111044 

Abdul Rahman Zain - NIM 245150400111013 

Kresna Wibowo Patebong - NIM 245150407111031 

# Program Studi Sistem Informasi 

# Jurusan Sistem Informasi 

# Fakultas Ilmu Komputer 

# Universitas Brawijaya 

# 2026 1. Tujuan Proyek (Project Objective) 

Proyek Eco-Retail ERP Analytics: Optimisasi Food Waste & Rantai Pasok Dingin dirancang untuk menjawab dua permasalahan operasional paling kritis yang dihadapi oleh jaringan supermarket modern: kerugian finansial akibat food waste produk segar dan beku, serta risiko kualitas produk yang timbul dari kegagalan pemantauan rantai pasok dingin (cold chain). Kedua masalah ini saling berkaitan — cold chain yang tidak terpantau dengan baik secara langsung mempercepat kerusakan produk dan meningkatkan angka waste, namun selama ini data dari dua domain tersebut tersimpan terpisah dan tidak pernah dianalisis secara terintegrasi. 

Proyek ini bertujuan membangun sebuah ekosistem Business Intelligence (BI) yang sepenuhnya berbasis teknologi open-source dan gratis, menggunakan dataset publik skala besar (jutaan baris) dari Kaggle sebagai sumber data utama. Arsitektur sistem mencakup DuckDB sebagai data warehouse lokal, Python Scheduler (Prefect) sebagai orkestrasi pipeline ETL yang ringan, dbt Core untuk transformasi data berlapis dengan arsitektur Medallion (Bronze, Silver, Gold), serta Metabase Community Edition sebagai platform visualisasi. Di atas fondasi ini, sistem juga mengintegrasikan model prediktif machine learning berbasis Python (Prophet dan Scikit-learn) untuk forecasting permintaan produk dan deteksi anomali cold chain. Seluruh sistem berjalan menggunakan Docker Compose di mesin lokal tanpa biaya infrastruktur apapun. 

Secara terukur, proyek ini menargetkan pengurangan food waste rate sebesar 30% dari baseline dataset, peningkatan cold chain compliance rate menjadi di atas 95%, serta eliminasi 90% proses pembuatan laporan manual. Model demand forecasting ditargetkan mencapai MAPE ≤15% yang diverifikasi menggunakan teknik backtesting walk-forward validation pada data historis. 2. Sasaran Pengguna (Target Audience) 

Sistem ini dirancang untuk melayani dua kelompok pengguna utama yang memiliki kebutuhan sangat berbeda dalam cara mereka mengonsumsi informasi bisnis. Memahami perbedaan kebutuhan ini adalah kunci dari desain sistem, karena dashboard yang tepat untuk seorang direktur adalah sesuatu yang justru terlalu dangkal bagi seorang data analyst — dan sebaliknya. 

2.1 Manajemen Eksekutif (VP, Direktur, General Manager) 

Kelompok pengguna pertama adalah jajaran manajemen eksekutif supermarket, termasuk Vice President of Operations, Direktur Supply Chain, dan General Manager toko atau regional. Mereka membutuhkan informasi dengan cepat, dalam format yang ringkas, dan langsung mengarah pada angka performa bisnis tanpa harus melakukan konfigurasi atau analisis manual. Pertanyaan yang mereka ajukan kepada sistem umumnya bersifat strategis: 'Apakah target pengurangan waste bulan ini tercapai?', 'Zona cold storage mana yang paling sering mengalami breach minggu ini?', atau 'Apakah ada risiko overstock yang perlu diperhatikan sebelum akhir pekan?' 

Untuk kelompok ini, sistem menyediakan high-level executive dashboard yang menampilkan KPI utama dalam format visual yang bersih: angka persentase waste, indikator compliance cold chain dengan kode warna merah/kuning/hijau, tren revenue, dan ringkasan prediksi demand untuk 7 hari ke depan. Akses eksekutif dibatasi pada tampilan summary dan tidak diberikan kemampuan untuk memodifikasi query atau mengakses data mentah, demi menjaga keamanan dan fokus informasi. 

2.2 Data Analyst / BI Analyst 

Kelompok pengguna kedua adalah tim data analyst dan BI analyst yang bertugas menggali lebih dalam ke dalam data untuk menghasilkan rekomendasi aksi yang lebih spesifik. Mereka adalah pengguna teknis yang membutuhkan akses ke data terperinci per SKU, per toko, per supplier, dan per periode waktu. Pertanyaan mereka lebih bersifat diagnostik dan investigatif: 'SKU mana yang paling banyak waste di kategori produk segar?', 'Apakah kenaikan waste minggu lalu berkorelasi dengan cold chain breach di zona freezer?', atau 'Supplier mana yang paling sering terlambat mengirim dan berapa dampaknya terhadap stok produk segar?' 

Untuk kelompok ini, sistem menyediakan analyst dashboard dengan kemampuan drill-down penuh dari level ringkasan (total waste per kategori) hingga level terkecil (transaksi individual per produk), filter multidimensi, tabel detail yang dapat di-ekspor ke CSV, serta akses langsung ke output model ML termasuk data prediksi demand dan riwayat anomali suhu cold chain. Analyst diberikan izin akses lebih luas di Metabase, termasuk kemampuan membuat pertanyaan kustom (custom questions) berbasis SQL yang terhubung ke DuckDB. 3. Sumber Data (Dataset) 

3.1 Dataset Utama — Skala Besar (Jutaan Baris) 

Proyek ini menggunakan dataset skala besar dengan total minimal 1 juta baris data lintas semua sumber. Dataset yang digunakan adalah: 

● Instacart Market Basket Analysis Dataset (Kaggle) — 3 juta+ transaksi belanja dari 200.000+ pengguna, mencakup data produk, aisle, department, dan urutan pembelian. Digunakan sebagai sumber utama data transaksi penjualan dan analisis demand. 

● M5 Forecasting - Accuracy Dataset (Kaggle/Walmart) — Data penjualan harian 30.490 produk dari 10 toko Walmart selama 5+ tahun (2011-2016), total puluhan juta baris. Digunakan untuk demand forecasting time-series per SKU. 

● Retail Data Warehouse – 12 Table 1M+ Rows Dataset (Kaggle) — Dataset data warehouse retail dengan 12 tabel terstruktur dan 1 juta+ baris, mencakup dimensi produk, toko, supplier, dan fakta transaksi. Digunakan sebagai backbone schema data warehouse. 

● DataCo Smart Supply Chain for Big Data Analysis (Kaggle) — Dataset supply chain dan logistics lengkap dengan 180.000+ records purchase order dan pengiriman. Digunakan untuk analisis supplier performance dan lead time. 

● Cold Chain Monitoring Dataset (Kaggle/UCI) — Dataset log suhu cold storage real dari sensor IoT. Digunakan untuk analisis cold chain compliance dan anomaly detection. 4. Komponen Utama (Key Features/Components) 

4.1 Arsitektur Medallion (Bronze, Silver, Gold) 

Proyek ini mengimplementasikan arsitektur data Medallion secara penuh menggunakan DuckDB dan dbt Core: 

● Bronze Layer: Penyimpanan data mentah dari semua sumber CSV/dataset. Data diingest apa adanya tanpa transformasi, mempertahankan struktur asli dataset. Tabel: bronze_instacart_orders, bronze_m5_sales, bronze_retail_dw_*, bronze_supply_chain, bronze_cold_chain. 

● Silver Layer: Hasil cleansing, standarisasi tipe data, deduplication, dan join antar sumber. Pada layer ini dilakukan penyatuan dataset dari berbagai sumber menjadi model data yang konsisten. Tabel: silver_dim_product, silver_dim_store, silver_dim_supplier, silver_dim_date, silver_fact_sales, silver_fact_inventory, silver_fact_cold_chain, silver_fact_purchase_order. 

● Gold Layer: Data siap analitik untuk dashboard dan model ML. Berisi tabel agregasi, mart, dan output prediktif. Tabel: gold_mart_food_waste_summary, gold_mart_cold_chain_compliance, gold_mart_demand_forecast, gold_mart_supplier_performance, gold_mart_executive_kpi. 

4.2 ETL Pipeline (Python Scheduler + dbt + DuckDB) 

Menggantikan Apache Airflow yang resource-heavy, orkestrasi pipeline menggunakan Prefect — sebuah Python-native workflow orchestrator yang jauh lebih ringan dan mudah dikonfigurasi untuk kebutuhan akademik. Prefect dapat berjalan sebagai proses tunggal tanpa memerlukan message broker atau worker pool terpisah. 

Pipeline terdiri dari tiga tahap utama: (1) Ingest — membaca semua file CSV dataset ke Bronze layer DuckDB, (2) Transform — menjalankan dbt run untuk membangun Silver dan Gold layer, dan (3) ML — menjalankan training/inference model prediktif dan menyimpan output ke Gold layer. 

4.3 Analitik Deskriptif 

Analitik deskriptif mencakup: ringkasan food waste rate per kategori produk dan per toko, tren penjualan historis, cold chain compliance scorecard, analisis supplier performance, dan distribusi demand per SKU. Semua hasil divisualisasikan di Metabase Executive dan Analyst Dashboard. 

4.4 Analitik Prediktif (Machine Learning) 

Komponen ML terdiri dari dua model: 

● Demand Forecasting per SKU menggunakan Facebook Prophet, ditraining dari dataset M5/Instacart dengan fitur seasonality harian dan mingguan, flag promosi, dan data historis 90+ hari. Output: prediksi permintaan 7 hari ke depan dengan confidence interval 80% dan 95%. Target akurasi: MAPE ≤15%. ● Cold Chain Anomaly Detection menggunakan Isolation Forest (Scikit-learn), ditraining dari Cold Chain Monitoring Dataset. Model membedakan dua tipe anomali: equipment breach (suhu naik gradual >30 menit) vs operational error (lonjakan sementara). Output tersimpan di gold_mart_cold_chain_compliance. 

4.5 Dashboard Interaktif (Metabase Community) 

Dua dashboard utama: Executive Dashboard (KPI ringkasan waste rate, cold chain compliance, revenue trend, prediksi demand 7 hari) dan Analyst Dashboard (drill-down per SKU, timeline cold chain breach, supplier analysis, visualisasi output ML dengan confidence interval). 

4.6 Role-Based Access Control (RBAC) 

Grup Eksekutif: akses terbatas pada dashboard summary. Grup Analyst: akses penuh termasuk custom SQL query dan ekspor data. Scheduled reporting PDF mingguan ke email manajemen setiap Senin pagi via fitur native Metabase Community. 5. Metrik Utama (KPIs) yang Akan Dilacak 

KPI  Baseline  Target (6 Bln)  Sumber Data 

Food Waste Rate  ~11–14%  ≤ 8% (−30%)  Instacart + M5 

Dataset 

Cold Chain 

Compliance 

~82–88%  ≥ 95%  Cold Chain 

Monitoring Dataset 

Demand Forecast 

Accuracy 

~22% (naive)  ≤ 15%  Prophet / 

Walk-forward 

validation 

Laporan Manual 

Tereliminasi 

100% manual  ≤ 10% manual  Metabase 

Auto-Report 

ETL Pipeline 

Latency 

- < 2 jam (full refresh)  Prefect Run Logs 

Supplier Lead Time 

Accuracy 

~70% on-time  Deviasi ≤ 1 hari  DataCo Supply Chain 

Dataset 

Volume Data 

Diproses 

74K rows  ≥1 juta rows  Multi-dataset 

combined 6. Kebutuhan Teknis (Technical Requirements) 

Kategori  Teknologi  Fungsi dalam Sistem 

Sumber Data  Dataset Kaggle (CSV) — skala juta 

baris 

Data transaksi, cold chain, supply 

chain, inventory dalam skala 

besar 

Data Warehouse  DuckDB (local, gratis)  In-process analytical DB, optimal 

untuk dataset ratusan juta baris 

tanpa cloud 

ETL 

Orchestration 

Prefect (Python-native, gratis)  Scheduling pipeline ringan, 

single-process, tidak butuh 

message broker 

Transformasi 

Data 

dbt Core (open-source)  Transformasi SQL berlapis 

Bronze→Silver→Gold dengan 

lineage otomatis 

Machine Learning  Prophet + Scikit-learn (Python)  Demand forecasting (Prophet) 

dan anomaly detection (Isolation 

Forest) 

Visualisasi / BI  Metabase Community (self-hosted)  Dashboard interaktif, RBAC, 

scheduled report, koneksi ke 

DuckDB 

Data Quality  Great Expectations (open-source)  Validasi kualitas data pada setiap 

layer ETL pipeline 

Infrastruktur  Localhost / Docker Compose  Seluruh sistem di mesin lokal — 

zero cost infrastructure 

Kebutuhan minimum hardware: RAM 8GB (16GB direkomendasikan), storage 30GB untuk dataset skala besar dan DuckDB, CPU 4 core. Semua service (Prefect, Metabase, DuckDB) didefinisikan dalam satu docker-compose.yml. 

Perubahan dari PRD sebelumnya: Apache Airflow diganti dengan Prefect karena Airflow memerlukan banyak container (scheduler, webserver, worker, Redis/PostgreSQL) sehingga sangat resource-heavy untuk mesin lokal mahasiswa. Prefect hanya memerlukan satu proses Python dengan UI built-in. 7. Sukses Kriteria (Success Metrics) 

Keberhasilan proyek diukur melalui dua dimensi: keberhasilan teknis dan dampak analitik. 

Dari sisi teknis: 

● Pipeline ETL mampu menyelesaikan full refresh dalam waktu <2 jam untuk dataset jutaan baris. 

● Seluruh pipeline berjalan otomatis tanpa intervensi manual minimal 5 hari berturut-turut. 

● Model demand forecasting mencapai MAPE ≤15% via walk-forward validation. 

● Arsitektur Medallion (Bronze, Silver, Gold) dapat didemonstrasikan secara penuh dengan data nyata. 

Dari sisi kualitas analitik: 

● Semua angka KPI di Metabase dapat diverifikasi terhadap data sumber CSV dengan selisih ≤0.1%. 

● Output model prediktif (demand forecast + anomaly detection) terintegrasi di dashboard Metabase. 

● Dashboard diakses oleh minimal 80% pengguna terdaftar setiap hari. 8. Timeline Proyek (Milestone) 

Proyek ini dijalankan dalam 5 minggu dengan empat anggota tim yang memiliki spesialisasi berbeda. Pembagian fase mencerminkan alur natural pengembangan sistem BI: dari setup fondasi data di minggu pertama, pembangunan pipeline dan dashboard di minggu kedua-ketiga, pengujian menyeluruh di minggu keempat, hingga go-live dan pelatihan di minggu kelima. Detail tugas harian per anggota tim tersedia dalam dokumen Sprint Plan terpisah (spreadsheet). 

Mingg 

u

Fase  Deliverable Utama  Penanggungjawab 

W1  Foundation 

& Data 

Setup 

Dataset besar diunduh & divalidasi 

(Instacart, M5, Retail DW), DuckDB 

schema Bronze siap, Prefect setup, 

dbt project init, wireframe final 

Nick (infra), Zain (data), 

Roeney (design), Kresna 

(KPI mapping) 

W2  Pipeline & 

Dashboard 

v1 

Bronze→Silver→Gold pipeline aktif 

via Prefect+dbt, Executive 

Dashboard v1 live, model ML v1 

ditraining dari data jutaan baris 

Zain (ETL/dbt), Roeney 

(dashboard), Nick 

(pipeline), Kresna (ML) 

W3  Integrasi & 

Testing 

Internal 

End-to-end integration selesai, 

RBAC aktif, cold chain dashboard 

live, output ML terintegrasi di 

Metabase, analitik deskriptif + 

prediktif lengkap 

Nick (integrasi), Roeney 

(cold chain UI), Zain 

(optimasi), Kresna (analitik) 

W4  UAT & 

Perbaikan 

UAT sign-off, semua bug Critical & 

High resolved, dokumentasi lengkap, 

presentasi insight bisnis dari data 

nyata 

Nick (UAT), Roeney (UX 

fix), Zain (data fix), Kresna 

(analytic QA) 

W5  Go-Live & 

Pelatihan 

Sistem live, demo arsitektur 

Medallion end-to-end, presentasi 

analitik deskriptif + prediktif, 

handover dokumentasi 

Semua anggota tim