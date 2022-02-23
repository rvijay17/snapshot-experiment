-- ----------------------------------------------------------------------------------------------------
-- Author       : Vijay Rajagopalan (s709081)
-- Description  : In Delta Loading, the Batch ID (or Batch Sequence ID) links the master_control_table
--                and the job_control_table. In the Delta Loading implementation using DBT,
--                we are using the DBT variable run_started_at as the Batch ID. It will be unique for
--                the current run.
--                This macro is to convert the epoch time to integer and use it as the Batch ID.
-- ----------------------------------------------------------------------------------------------------
-- VERSIONS  DATE          WHO                      DESCRIPTION
-- 1.00                    Vijay Rajagopalan        Initial Release
-------------------------------------------------------------------------------------------------------

{% macro mu_delta_load_get_batch_id() -%} 

{% if execute %}
    {{  run_started_at.timestamp()|round|int }}
{% endif %}

{%- endmacro %}
