WITH source AS (
    SELECT * FROM {{ source('eco_retail_bronze', 'iot_telemetry') }}
)
SELECT * FROM source