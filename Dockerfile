# Menggunakan versi Airflow yang sama, tapi dengan Python 3.10
FROM apache/airflow:2.8.1-python3.10

# Update pip terlebih dahulu untuk menghindari warning
RUN pip install --upgrade pip

# Install duckdb, pandas, dan dbt
RUN pip install --no-cache-dir duckdb pandas dbt-core dbt-duckdb