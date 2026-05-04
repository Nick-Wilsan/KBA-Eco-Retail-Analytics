
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and temperature_c >= -50.0 and temperature_c <= 60.0
)
 as expression


    from "warehouse"."silver"."silver_fact_cold_chain"
    

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors








  
  
      
    ) dbt_internal_test