
  
    
    

    create  table
      "warehouse"."warehouse_silver"."silver_fact_purchase_order__dbt_tmp"
  
    as (
      WITH po_data AS (
    SELECT
        CAST(order_id AS VARCHAR) AS po_id,
        CAST(order_date AS DATE) AS order_date_id,
        CAST(shipping_date AS DATE) AS shipping_date_id,
        CAST(order_quantity AS INTEGER) AS order_qty,
        CAST(sales_amount AS DOUBLE) AS sales_amount,
        CAST("Days for shipping (real)" AS INTEGER) AS actual_shipping_days,
        CAST("Days for shipment (scheduled)" AS INTEGER) AS scheduled_shipping_days,
        "Delivery Status" AS delivery_status,
        CAST("Late_delivery_risk" AS BOOLEAN) AS is_late_risk,
        "Shipping Mode" AS shipping_mode
    FROM "warehouse"."warehouse"."stg_supply_chain"
    WHERE order_id IS NOT NULL
)

SELECT * FROM po_data
    );
  
  