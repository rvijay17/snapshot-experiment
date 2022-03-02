
{{ config(materialized='view', tags=["viewtest"]) }}

with 
customer_v1 as ( select * from {{ ref('customer_v1') }}),
customer_v2 as ( select * from {{ ref('customer_v2') }}),
customer_v3 as ( select * from {{ ref('customer_v3') }}),
customer_v4 as ( select * from {{ ref('customer_v4') }}),
customer_v5 as ( select * from {{ ref('customer_v5') }}),
customer_v6 as ( select * from {{ ref('customer_v6') }}),
customer_v7 as ( select * from {{ ref('customer_v7') }}),
customer_v8 as ( select * from {{ ref('customer_v8') }}),
customer_v9 as ( select * from {{ ref('customer_v9') }}),
customer_v10 as ( select * from {{ ref('customer_v10') }}),

final as (

    select * from customer_v1
    union all
    select * from customer_v2
    union all
    select * from customer_v3
    union all
    select * from customer_v4
    union all
    select * from customer_v5
    union all
    select * from customer_v6
    union all
    select * from customer_v7
    union all
    select * from customer_v8
    union all
    select * from customer_v9
    union all
    select * from customer_v10
)

select * from final
