
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

with all_values as (

    select
        waste_rate_status as value_field

    from "warehouse"."gold"."gold_mart_food_waste_summary"
    

),
set_values as (

    select
        cast('Healthy' as TEXT) as value_field
    union all
    select
        cast('Moderate Risk' as TEXT) as value_field
    union all
    select
        cast('High Waste Risk' as TEXT) as value_field
    union all
    select
        cast('No Stock' as TEXT) as value_field
    
    
),
validation_errors as (
    -- values from the model that are not in the set
    select
        v.value_field
    from
        all_values v
        left join
        set_values s on v.value_field = s.value_field
    where
        s.value_field is null

)

select *
from validation_errors


  
  
      
    ) dbt_internal_test