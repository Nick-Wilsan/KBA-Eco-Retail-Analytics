
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select device_mac
from "warehouse"."bronze"."iot_telemetry"
where device_mac is null



  
  
      
    ) dbt_internal_test