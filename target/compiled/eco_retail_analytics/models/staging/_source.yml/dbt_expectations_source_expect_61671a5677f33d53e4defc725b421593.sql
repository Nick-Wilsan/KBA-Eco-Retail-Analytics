



    with grouped_expression as (
    select
        
        
    
  
( 1=1 and count(*) >= 1000 and count(*) <= 10000000
)
 as expression


    from "warehouse"."bronze"."m5_forcasting"
    

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





