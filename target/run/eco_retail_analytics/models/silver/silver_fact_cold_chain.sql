
  
    
    

    create  table
      "warehouse"."silver"."silver_fact_cold_chain__dbt_tmp"
  
    as (
      WITH iot_data AS (
    SELECT
        -- Parse timezone-aware timestamp
        CAST(timestamp + INTERVAL 5 YEAR + INTERVAL 10 MONTH AS TIMESTAMP) AS telemetry_timestamp,
        CAST(timestamp + INTERVAL 5 YEAR + INTERVAL 10 MONTH AS DATE) AS date_id,
        device_mac AS device_id,
        CAST(temp_celsius AS DOUBLE) AS temperature_c,
        CAST(humidity_pct AS DOUBLE) AS humidity_percentage,
        CAST(co_level AS DOUBLE) AS co_level,
        CAST(smoke_level AS DOUBLE) AS smoke_level,
        CAST(is_light AS BOOLEAN) AS is_light_on,
        CAST(is_motion AS BOOLEAN) AS is_motion_detected
    FROM "warehouse"."warehouse"."stg_iot_telemetry"
    -- Filter out anomali ekstrim jika sensor rusak (misal suhu di bawah -100 atau di atas 100)
    WHERE temp_celsius BETWEEN -100 AND 100
)

SELECT * FROM iot_data
ORDER BY telemetry_timestamp
    );
  
  