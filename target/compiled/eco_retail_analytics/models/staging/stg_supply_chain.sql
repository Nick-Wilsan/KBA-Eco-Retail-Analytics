WITH source AS (
    -- Pastikan nama 'SupplyChain' persis dengan nama tabel di DuckDB Anda (case-sensitive)
    SELECT * FROM "warehouse"."bronze"."SupplyChain"
)
SELECT * FROM source