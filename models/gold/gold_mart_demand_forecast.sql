WITH daily_sales AS (
    SELECT 
        order_date_id AS date_id,
        product_id,
        store_id,
        SUM(quantity) AS daily_sales_qty,
        SUM(total_payment) AS daily_revenue,
        COUNT(DISTINCT order_id) AS total_orders
    FROM {{ ref('silver_fact_sales') }}
    GROUP BY 1, 2, 3
),
forecast_logic AS (
    SELECT 
        ds.*,
        -- Menghitung rata-rata penjualan sebagai forecast sederhana
        AVG(daily_sales_qty) OVER(PARTITION BY product_id, store_id) AS forecast_qty,
        -- Menghitung standar deviasi untuk batas atas dan bawah (Confidence Interval)
        STDDEV(daily_sales_qty) OVER(PARTITION BY product_id, store_id) AS sales_stddev
    FROM daily_sales ds
)

SELECT 
    d.date_id,
    d.year,
    d.month,
    d.day_name,
    fl.store_id,
    fl.product_id,
    p.category_name,
    fl.daily_sales_qty,
    fl.daily_revenue,
    fl.total_orders,
    
    -- Kolom yang diminta Nick Wilsan:
    ROUND(fl.forecast_qty, 2) AS forecast_qty,
    ROUND(fl.forecast_qty - (1.28 * COALESCE(fl.sales_stddev, 0)), 2) AS lower_bound_80,
    ROUND(fl.forecast_qty + (1.28 * COALESCE(fl.sales_stddev, 0)), 2) AS upper_bound_80,
    
    -- MAPE Contribution (Persentase error sederhana)
    CASE 
        WHEN fl.daily_sales_qty = 0 THEN 0
        ELSE ROUND(ABS(fl.daily_sales_qty - fl.forecast_qty) / fl.daily_sales_qty, 4)
    END AS mape_contribution

FROM forecast_logic fl
JOIN {{ ref('silver_dim_date') }} d 
    ON fl.date_id = d.date_id
LEFT JOIN {{ ref('silver_dim_product') }} p
    ON fl.product_id = p.product_id
