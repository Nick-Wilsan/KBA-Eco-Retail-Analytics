

WITH store_data AS (
    SELECT 
        CAST(store_id AS VARCHAR) AS store_id,
        store_city AS city,
        CAST(NULL AS VARCHAR) AS state
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
),
-- Mengambil nilai unik dan mengisi kolom null dengan partisi jika tersedia
deduplicated_stores AS (
    SELECT DISTINCT 
        store_id, 
        MAX(city) OVER(PARTITION BY store_id) AS city,
        MAX(state) OVER(PARTITION BY store_id) AS state
    FROM store_data
)

SELECT 
    store_id,
    CASE 
        -- Mapping Kode Negara Bagian (M5 Dataset) agar lebih deskriptif
        WHEN store_id LIKE 'CA_%' THEN REPLACE(store_id, 'CA_', 'California_')
        WHEN store_id LIKE 'TX_%' THEN REPLACE(store_id, 'TX_', 'Texas_')
        WHEN store_id LIKE 'WI_%' THEN REPLACE(store_id, 'WI_', 'Wisconsin_')
        
        -- Mapping ID Numerik ke Nama Lokasi (Retail Dataset)
        WHEN store_id = '37' THEN 'Mumbai Central'
        WHEN store_id = '51' THEN 'Bangalore Square'
        WHEN store_id = '45' THEN 'Delhi North'
        
        -- Fallback: Gunakan format Nama Kota atau format Store ID asli
        ELSE COALESCE(city || ' Store', 'Store ' || store_id)
    END AS store_name,
    city,
    state
FROM deduplicated_stores