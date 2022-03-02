{% snapshot source_cc_transaction %}

{{
    config(
      tags=["source"],
      unique_key='id',
      pre_hook=before_begin("{{ mu_delta_load_job_control_start('" ~ this.identifier ~ "') }}"),
      post_hook="ANALYZE {{ this }}",
      target_schema='dev_evan',
      strategy='timestamp',
      updated_at='updatetime'
    )
}}

    select 
        *
        , {{mu_delta_load_get_batch_id()}}::BIGINT as batch_id
    from {{ source('dev_evan', 'cc_transaction') }}

{% endsnapshot %}