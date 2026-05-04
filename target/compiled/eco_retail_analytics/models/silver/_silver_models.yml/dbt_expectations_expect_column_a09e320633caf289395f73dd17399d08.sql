






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and humidity_percentage >= 0 and humidity_percentage <= 100
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







