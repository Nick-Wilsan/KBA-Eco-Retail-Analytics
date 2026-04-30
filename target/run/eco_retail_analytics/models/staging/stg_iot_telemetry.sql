
  
  create view "warehouse"."warehouse"."stg_iot_telemetry__dbt_tmp" as (
    WITH source AS (
    SELECT * FROM "warehouse"."bronze"."iot_telemetry"
)
SELECT * FROM source
  );
