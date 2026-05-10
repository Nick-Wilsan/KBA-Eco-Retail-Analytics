
  
    
    

    create  table
      "warehouse"."silver"."silver_dim_product__dbt_tmp"
  
    as (
      

WITH retail_products AS (
    SELECT 
        CAST(product_id AS VARCHAR) AS product_id,
        category_name,
        CAST(unit_price AS DOUBLE) AS default_price,
        'Retail' AS source_system
    FROM "warehouse"."warehouse"."stg_retail_data"
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
    CASE 
        -- Pemetaan 30 Kategori Supermarket (Lengkap)
        WHEN category_name = 'Cat_1' THEN 'Pantry & Dry Goods'
        WHEN category_name = 'Cat_2' THEN 'Snacks & Sweets'
        WHEN category_name = 'Cat_3' THEN 'Meat & Seafood'
        WHEN category_name = 'Cat_4' THEN 'Canned Goods'
        WHEN category_name = 'Cat_5' THEN 'Fresh Produce'
        WHEN category_name = 'Cat_6' THEN 'Baking Needs'
        WHEN category_name = 'Cat_7' THEN 'Breakfast & Cereal'
        WHEN category_name = 'Cat_8' THEN 'Dairy & Eggs'
        WHEN category_name = 'Cat_9' THEN 'Beverages'
        WHEN category_name = 'Cat_10' THEN 'Condiments & Sauces'
        
        WHEN category_name = 'Cat_11' THEN 'Deli & Cheese'
        WHEN category_name = 'Cat_12' THEN 'Frozen Foods'
        WHEN category_name = 'Cat_13' THEN 'Personal Care'
        WHEN category_name = 'Cat_14' THEN 'Health & Wellness'
        WHEN category_name = 'Cat_15' THEN 'Household Essentials'
        WHEN category_name = 'Cat_16' THEN 'Baby Care'
        WHEN category_name = 'Cat_17' THEN 'Pet Care'
        WHEN category_name = 'Cat_18' THEN 'Paper Goods'
        WHEN category_name = 'Cat_19' THEN 'Cleaning Supplies'
        WHEN category_name = 'Cat_20' THEN 'Beauty & Cosmetics'
        
        WHEN category_name = 'Cat_21' THEN 'Office & School Supplies'
        WHEN category_name = 'Cat_22' THEN 'Electronics & Accessories'
        WHEN category_name = 'Cat_23' THEN 'Home & Garden'
        WHEN category_name = 'Cat_24' THEN 'Toys & Games'
        WHEN category_name = 'Cat_25' THEN 'Apparel & Accessories'
        WHEN category_name = 'Cat_26' THEN 'Bakery'
        WHEN category_name = 'Cat_27' THEN 'Floral & Gifts'
        WHEN category_name = 'Cat_28' THEN 'Prepared Foods (Ready-to-Eat)'
        WHEN category_name = 'Cat_29' THEN 'Spices & Seasonings'
        WHEN category_name = 'Cat_30' THEN 'International Foods'
        
        ELSE COALESCE(category_name, 'Uncategorized')
    END AS category_name,
    default_price,
    source_system
FROM deduplicated_products
WHERE rn = 1
    );
  
  