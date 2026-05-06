WITH daily_telemetry AS (
    SELECT 
        date_id,
        device_id,
        AVG(temperature_c) AS avg_temp,
        MAX(temperature_c) AS max_temp,
        MIN(temperature_c) AS min_temp,
        AVG(humidity_percentage) AS avg_humidity,
        -- Menghitung berapa kali suhu melebihi batas aman (misal: > 5 derajat Celcius untuk Cold Chain)
        SUM(CASE WHEN temperature_c > 5.0 THEN 1 ELSE 0 END) AS temperature_violations
    FROM "warehouse"."silver"."silver_fact_cold_chain"
    GROUP BY 1, 2
)

SELECT 
    date_id,
    device_id,
    ROUND(avg_temp, 2) AS avg_temp_c,
    ROUND(max_temp, 2) AS max_temp_c,
    ROUND(min_temp, 2) AS min_temp_c,
    ROUND(avg_humidity, 2) AS avg_humidity_pct,
    temperature_violations,
    CASE 
        WHEN temperature_violations > 0 THEN 'Non-Compliant'
        ELSE 'Compliant'
    END AS daily_compliance_status
FROM daily_telemetry