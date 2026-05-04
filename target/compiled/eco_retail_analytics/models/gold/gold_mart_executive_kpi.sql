WITH monthly_sales AS (
    SELECT 
        DATE_TRUNC('month', order_date_id) AS kpi_month,
        SUM(total_payment) AS total_revenue,
        SUM(quantity) AS total_items_sold
    FROM "warehouse"."silver"."silver_fact_sales"
    GROUP BY 1
),
monthly_waste AS (
    SELECT 
        DATE_TRUNC('month', date_id) AS kpi_month,
        SUM(potential_waste_value) AS total_potential_waste_value
    FROM "warehouse"."gold"."gold_mart_food_waste_summary"
    GROUP BY 1
),
monthly_cold_chain AS (
    SELECT 
        DATE_TRUNC('month', date_id) AS kpi_month,
        SUM(temperature_violations) AS total_temp_violations
    FROM "warehouse"."gold"."gold_mart_cold_chain_compliance"
    GROUP BY 1
)

SELECT 
    CAST(s.kpi_month AS DATE) AS kpi_month,
    s.total_revenue,
    s.total_items_sold,
    COALESCE(w.total_potential_waste_value, 0) AS total_potential_waste_value,
    COALESCE(c.total_temp_violations, 0) AS total_temp_violations
FROM monthly_sales s
LEFT JOIN monthly_waste w ON s.kpi_month = w.kpi_month
LEFT JOIN monthly_cold_chain c ON s.kpi_month = c.kpi_month
ORDER BY kpi_month DESC