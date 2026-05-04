






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and waste_rate_pct >= 0.0 and waste_rate_pct <= 100.0
)
 as expression


    from "warehouse"."gold"."gold_mart_food_waste_summary"
    

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







