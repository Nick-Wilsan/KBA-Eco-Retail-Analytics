WITH retail_products AS (
    SELECT 
        CAST(product_id AS VARCHAR) AS product_id,
        category_name,
        CAST(unit_price AS DOUBLE) AS default_price,
        'Retail' AS source_system
    FROM {{ ref('stg_retail_data') }}
    WHERE product_id IS NOT NULL
),
-- Menghapus duplikasi produk agar ID benar-benar unik (Primary Key)
deduplicated_products AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY default_price DESC) as rn
    FROM retail_products
)

SELECT 
    product_id,
    category_name,
    default_price,
    source_system
FROM deduplicated_products
WHERE rn = 1
