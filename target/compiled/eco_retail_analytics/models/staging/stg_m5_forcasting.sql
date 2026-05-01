WITH source AS (
    SELECT * FROM "warehouse"."bronze"."m5_forcasting"
)
SELECT * FROM source