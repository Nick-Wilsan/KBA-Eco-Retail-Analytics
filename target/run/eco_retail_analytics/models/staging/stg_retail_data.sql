
  
  create view "warehouse"."warehouse"."stg_retail_data__dbt_tmp" as (
    WITH source AS (
    SELECT * FROM "warehouse"."bronze"."retail_data"
)
SELECT * FROM source
  );
