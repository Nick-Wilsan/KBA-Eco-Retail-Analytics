
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    "Order Item Id" as unique_field,
    count(*) as n_records

from "warehouse"."bronze"."SupplyChain"
where "Order Item Id" is not null
group by "Order Item Id"
having count(*) > 1



  
  
      
    ) dbt_internal_test