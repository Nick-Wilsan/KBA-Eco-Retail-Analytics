WITH ml_anomalies AS (
    SELECT 
        CAST(telemetry_timestamp AS DATE) AS date_id,
        device_id,
        COUNT(*) AS total_readings,
        -- Menjumlahkan angka 1 (breach) dari AI Kresna menjadi total count
        SUM(equipment_breach) AS equipment_breach_count,
        -- Mengambil rata-rata persentase kepatuhan dari AI
        AVG(compliance_rate_pct) AS compliance_rate_pct,
        AVG(temperature_c) AS avg_temp_saat_anomali
    FROM {{ ref('gold_anomaly_check') }}
    GROUP BY 1, 2
)

SELECT 
    date_id,
    device_id,
    total_readings,
    equipment_breach_count,
    ROUND(compliance_rate_pct, 2) AS compliance_rate_pct,
    
    -- Logika untuk menentukan tipe anomali dominan
    CASE 
        WHEN equipment_breach_count = 0 THEN 'Normal (No Anomaly)'
        WHEN avg_temp_saat_anomali > 5.0 THEN 'Temperature Breach'
        ELSE 'Humidity / Unknown Anomaly'
    END AS anomaly_type_dominant

FROM ml_anomalies
ORDER BY date_id DESC, equipment_breach_count DESC