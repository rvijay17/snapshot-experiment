{% snapshot source_cc_transaction %}

{{
    config(
      tags=["source"],
      unique_key='id',
      target_schema='dev_evan',
      strategy='timestamp',
      updated_at='updatetime'
    )
}}

select {{ mu_delta_load_insert_metadata('sor_cc_ci', 'cc_transaction', 'retired') }} , 
       * 
from {{ source('dev_evan', 'cc_transaction') }}

{% endsnapshot %}