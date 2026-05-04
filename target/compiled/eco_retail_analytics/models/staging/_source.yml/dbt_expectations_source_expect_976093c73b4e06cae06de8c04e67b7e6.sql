



    with grouped_expression as (
    select
        
        
    
  
( 1=1 and count(*) >= 100 and count(*) <= 5000000
)
 as expression


    from "warehouse"."bronze"."retail_data"
    

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





