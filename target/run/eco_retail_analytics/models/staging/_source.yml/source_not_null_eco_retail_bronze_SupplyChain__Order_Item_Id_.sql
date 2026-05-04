
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select "Order Item Id"
from "warehouse"."bronze"."SupplyChain"
where "Order Item Id" is null



  
  
      
    ) dbt_internal_test