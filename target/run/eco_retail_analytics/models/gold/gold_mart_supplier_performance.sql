
  
    
    

    create  table
      "warehouse"."warehouse_gold"."gold_mart_supplier_performance__dbt_tmp"
  
    as (
      WITH po_metrics AS (
    SELECT 
        -- Ekstrak bulan dan tahun untuk agregasi performa
        DATE_TRUNC('month', order_date_id) AS performance_month,
        delivery_status,
        shipping_mode,
        is_late_risk,
        (actual_shipping_days - scheduled_shipping_days) AS delay_days
    FROM "warehouse"."warehouse_silver"."silver_fact_purchase_order"
)

SELECT 
    CAST(performance_month AS DATE) AS performance_month,
    shipping_mode,
    COUNT(*) AS total_shipments,
    SUM(CASE WHEN is_late_risk = TRUE THEN 1 ELSE 0 END) AS total_late_shipments,
    ROUND(SUM(CASE WHEN is_late_risk = TRUE THEN 1.0 ELSE 0.0 END) / COUNT(*) * 100, 2) AS late_delivery_rate_pct,
    ROUND(AVG(CASE WHEN delay_days > 0 THEN delay_days ELSE 0 END), 2) AS avg_delay_days
FROM po_metrics
GROUP BY 1, 2
    );
  
  