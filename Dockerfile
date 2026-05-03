# Menggunakan image resmi Prefect
FROM prefecthq/prefect:2-python3.10

# Update pip
RUN pip install --upgrade pip

# Install duckdb, pandas, dan dbt untuk kebutuhan Zain & Kresna
RUN pip install --no-cache-dir duckdb pandas dbt-core dbt-duckdb

# Install Prophet untuk kebutuhan forecasting
RUN pip install prophet