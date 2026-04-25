import duckdb
import time

# Catat waktu mulai
start_time = time.time()

# Membuat koneksi ke database Anda
con = duckdb.connect('instacart_project.duckdb')

# Query yang sudah diperbaiki strukturnya: COPY membungkus seluruh WITH
query_gabung_semua = """
COPY (
    WITH all_orders AS (
        SELECT
            CAST(order_id AS INTEGER) AS order_id,
            CAST(user_id AS INTEGER) AS user_id,
            eval_set,
            CAST(order_number AS INTEGER) AS order_number,
            CAST(order_dow AS INTEGER) AS order_dow,
            CAST(order_hour_of_day AS INTEGER) AS order_hour_of_day,
            COALESCE(CAST(days_since_prior_order AS FLOAT), 0.0) AS days_since_prior_order
        FROM read_csv_auto('orders_*.csv')
    ),

    all_order_products AS (
        SELECT * FROM read_csv_auto('order_products_prior_*.csv')
        UNION ALL
        SELECT * FROM read_csv_auto('order_products__train.csv')
    ),

    merged_data AS (
        SELECT
            o.order_id, o.user_id, o.eval_set, o.order_number, o.order_dow,
            o.order_hour_of_day, o.days_since_prior_order,
            op.product_id, 
            CAST(op.add_to_cart_order AS INTEGER) AS add_to_cart_order,
            CAST(op.reordered AS INTEGER) AS reordered,
            p.product_name, d.department, a.aisle
        FROM all_orders o
        JOIN all_order_products op ON o.order_id = op.order_id
        JOIN read_csv_auto('products.csv') p ON op.product_id = p.product_id
        JOIN read_csv_auto('departments.csv') d ON p.department_id = d.department_id
        JOIN read_csv_auto('aisles.csv') a ON p.aisle_id = a.aisle_id
    ),

    deduplicated_data AS (
        SELECT DISTINCT * FROM merged_data
    )

    -- SELECT akhir yang wajib ada di dalam blok WITH sebelum di-COPY
    SELECT * FROM deduplicated_data
    USING SAMPLE 1000000
    
) TO 'instacart_cleaned_sample.csv' (HEADER, DELIMITER ',');
"""

print("⏳ Memulai proses membaca, menggabungkan, dan membersihkan jutaan baris data...")
print("Mohon tunggu, ini mungkin memakan waktu beberapa saat tergantung spesifikasi komputer Anda.")

# Eksekusi query
con.execute(query_gabung_semua)

# Hitung durasi eksekusi
end_time = time.time()
durasi = round(end_time - start_time, 2)

print(f"✅ Selesai dalam {durasi} detik!")
print("Silakan cek folder Anda, file 'instacart_cleaned_sample.csv' sudah berhasil dibuat.")