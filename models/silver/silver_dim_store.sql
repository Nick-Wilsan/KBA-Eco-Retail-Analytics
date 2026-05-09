WITH store_data AS (
    SELECT 
        CAST(store_id AS VARCHAR) AS store_id,
        store_city AS city,
        CAST(NULL AS VARCHAR) AS state
    FROM {{ ref('stg_retail_data') }}
    WHERE store_id IS NOT NULL
    
    UNION 
    
    SELECT 
        CAST(store_id AS VARCHAR) AS store_id,
        CAST(NULL AS VARCHAR) AS city,
        state_id AS state
    FROM {{ ref('stg_m5_forcasting') }}
    WHERE store_id IS NOT NULL
),
deduplicated_stores AS (
    SELECT DISTINCT 
        store_id, 
        MAX(city) OVER(PARTITION BY store_id) AS city,
        MAX(state) OVER(PARTITION BY store_id) AS state
    FROM store_data
)

SELECT 
    store_id,
    -- Mapping Kode Toko menjadi Nama Negara Bagian (US)
    CASE store_id
        WHEN 'CA_1' THEN 'California_1'
        WHEN 'CA_2' THEN 'California_2'
        WHEN 'CA_3' THEN 'California_3'
        WHEN 'CA_4' THEN 'California_4'
        WHEN 'TX_1' THEN 'Texas_1'
        WHEN 'TX_2' THEN 'Texas_2'
        WHEN 'TX_3' THEN 'Texas_3'
        WHEN 'WI_1' THEN 'Wisconsin_1'
        WHEN 'WI_2' THEN 'Wisconsin_2'
        WHEN 'WI_3' THEN 'Wisconsin_3'
        -- Jika tidak cocok dengan list di atas, gunakan format Kota
        ELSE COALESCE(city || ' Store', 'Store ' || store_id)
    END AS store_name,
    city,
    state
FROM deduplicated_stores