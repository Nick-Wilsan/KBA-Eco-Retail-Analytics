WITH supplier_data AS (
    SELECT DISTINCT
        supplier_country AS country,
        'Active' AS supplier_status
    FROM {{ ref('stg_retail_data') }}
    WHERE supplier_country IS NOT NULL
)

SELECT 
    -- Membuat ID unik buatan untuk supplier berdasarkan nama negaranya
    MD5(country) AS supplier_id, 
    country,
    supplier_status
FROM supplier_data
