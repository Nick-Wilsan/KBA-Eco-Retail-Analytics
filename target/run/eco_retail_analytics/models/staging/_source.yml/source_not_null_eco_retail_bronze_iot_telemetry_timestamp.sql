
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select timestamp
from "warehouse"."bronze"."iot_telemetry"
where timestamp is null



  
  
      
    ) dbt_internal_test