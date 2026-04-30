
  
  create view "warehouse"."warehouse"."stg_m5_forcasting__dbt_tmp" as (
    WITH source AS (
    SELECT * FROM "warehouse"."bronze"."m5_forcasting"
)
SELECT * FROM source
  );
