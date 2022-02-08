{% snapshot intg_claim_transaction_batch %}

{{
    config(
      tags=["intgclaimbatch"],
      unique_key='claim_transaction_key',
      target_schema='dev_evan',
      strategy='check',
      check_cols=['ts_updatetime', 't_updatetime', 'tl_updatetime'],
      updated_at='updatetime'
    )
}}
-- use the timestamps to determine if there are any differences
-- but, use the batch timestamp as the timestamp

select concat(ts.id, '~', t.id, '~', tl.id) as claim_transaction_key
    , ts.trandate   as trans_date
    , ts.transet    as trans_set_type
    , ts.userid     as trans_set_userid
    , ts.updatetime as ts_updatetime

    , t."type"      as trans_type
    , t.auth        as trans_authorised
    , t.updatetime  as t_updatetime

    , tl."desc"     as trans_desc
    , tl."amount"   as trans_amount
    , tl.updatetime as tl_updatetime
    , to_timestamp('{{ var("batch_timestamp") }}', 'YYYY-MM-DD HH24:MI:SS')::timestamp as updatetime
    
from {{ ref('source_cc_transaction_set') }} ts 
  left join {{ ref('source_cc_transaction') }} t on ts.id = t.transetid
  left join {{ ref('source_cc_transaction_line') }} tl on t.id = tl.tranid
where to_timestamp('{{ var("batch_timestamp") }}', 'YYYY-MM-DD HH24:MI:SS')::timestamp between ts.dbt_valid_from and coalesce(ts.dbt_valid_to,'9999-12-31 23:59:59')
  and to_timestamp('{{ var("batch_timestamp") }}', 'YYYY-MM-DD HH24:MI:SS')::timestamp between t.dbt_valid_from  and coalesce(t.dbt_valid_to,'9999-12-31 23:59:59')
  and to_timestamp('{{ var("batch_timestamp") }}', 'YYYY-MM-DD HH24:MI:SS')::timestamp between tl.dbt_valid_from and coalesce(tl.dbt_valid_to,'9999-12-31 23:59:59')

{% endsnapshot %}