WITH source AS (
    SELECT * FROM {{ source('eco_retail_bronze', 'retail_data') }}
)
SELECT * FROM source