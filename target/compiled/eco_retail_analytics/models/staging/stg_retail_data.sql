WITH source AS (
    SELECT * FROM "warehouse"."bronze"."retail_data"
)
SELECT * FROM source