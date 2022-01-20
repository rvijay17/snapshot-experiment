{% snapshot source_cc_transaction_line %}

{{
    config(
      tags=["source"],
      unique_key='id',
      target_schema='dev_nat',
      strategy='timestamp',
      updated_at='updatetime'
    )
}}

select * from {{ source('dev_nat', 'cc_transaction_line') }}

{% endsnapshot %}