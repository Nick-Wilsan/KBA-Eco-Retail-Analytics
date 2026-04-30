WITH source AS (
    SELECT * FROM "warehouse"."bronze"."iot_telemetry"
)
SELECT * FROM source