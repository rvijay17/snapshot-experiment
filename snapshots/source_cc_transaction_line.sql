{% snapshot source_cc_transaction_line %}

{{
    config(

      unique_key='id',
      target_schema='dev_evan',
      strategy='timestamp',
      updated_at='updatetime'
    )
}}

select * from {{ source('dev_evan', 'cc_transaction_line') }}

{% endsnapshot %}