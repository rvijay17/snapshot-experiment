{{
    config(
        materialized='incremental'
      , tags=["incr"]
      , unique_key='claim_pk'
    )
}}

select
      claim_transaction_key
    , trans_date
    , trans_set_type
    , trans_set_userid
    , ts_updatetime

    , trans_type
    , trans_authorised
    , t_updatetime

    , trans_desc
    , trans_amount
    , tl_updatetime
    , updatetime
    , dbt_valid_from
    , dbt_valid_to
    , dbt_updated_at
    , concat(claim_transaction_key, '~', dbt_valid_from) as claim_pk
    , coalesce(dbt_valid_to, dbt_updated_at) as claim_last_updated

from {{ ref('intg_claim_transaction_batch') }} 

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where coalesce(dbt_valid_to, dbt_updated_at) > (select max(claim_last_updated) from {{ this }})

{% endif %}