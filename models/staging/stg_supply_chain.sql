WITH source AS (
    -- Pastikan nama 'SupplyChain' persis dengan nama tabel di DuckDB Anda (case-sensitive)
    SELECT * FROM {{ source('eco_retail_bronze', 'SupplyChain') }}
)
SELECT * FROM source