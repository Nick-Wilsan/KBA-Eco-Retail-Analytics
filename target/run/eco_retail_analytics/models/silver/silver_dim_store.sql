
  
    
    

    create  table
      "warehouse"."warehouse_silver"."silver_dim_store__dbt_tmp"
  
    as (
      WITH store_data AS (
    SELECT 
        CAST(store_id AS VARCHAR) AS store_id,
        store_city AS city,
        CAST(NULL AS VARCHAR) AS state -- Placeholder jika state tidak ada di retail_data
    FROM "warehouse"."warehouse"."stg_retail_data"
    WHERE store_id IS NOT NULL
    
    UNION 
    
    -- Mengambil store dari m5 forecasting
    SELECT 
        CAST(store_id AS VARCHAR) AS store_id,
        CAST(NULL AS VARCHAR) AS city,
        state_id AS state
    FROM "warehouse"."warehouse"."stg_m5_forcasting"
    WHERE store_id IS NOT NULL
)

SELECT DISTINCT 
    store_id, 
    MAX(city) OVER(PARTITION BY store_id) AS city,
    MAX(state) OVER(PARTITION BY store_id) AS state
FROM store_data
    );
  
  