
{{ config(materialized='view', tags=["viewtest"]) }}

with 
customer_final as ( select * from {{ ref('customer_final') }}),


final as (

    select * from customer_final
)

select * from final
