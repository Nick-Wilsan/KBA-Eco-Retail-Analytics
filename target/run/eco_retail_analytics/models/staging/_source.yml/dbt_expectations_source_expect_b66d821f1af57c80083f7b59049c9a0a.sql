
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



    with grouped_expression as (
    select
        
        
    
  
( 1=1 and count(*) >= 1000 and count(*) <= 5000000
)
 as expression


    from "warehouse"."bronze"."instacart_grocery"
    

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