WITH source AS (
    SELECT * FROM {{ source('eco_retail_bronze', 'm5_forcasting') }}
)
SELECT * FROM source