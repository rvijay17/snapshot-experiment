-- ----------------------------------------------------------------------------------------------------
-- Author       : Vijay Rajagopalan (s709081)
-- Description  : In Delta Loading, this macro is used to add entries to the dbt_delta_load_master_control table 
--                For each batch, there will be a single entry in the table with status = 'started'.
-- ----------------------------------------------------------------------------------------------------
-- VERSIONS  DATE          WHO                      DESCRIPTION
-- 1.00                    Vijay Rajagopalan        Initial Release
-- 1.01      2021-12-10    Long Vu                  Change delta_load_master_control to dbt_delta_load_master_control
-- 1.02      2021-12-16    Cindy Chow               Add iag_common prefix to macro name mu_delta_load_get_batch_id
-------------------------------------------------------------------------------------------------------

{% macro mu_delta_load_master_control_start() -%}

{% if execute %}

    {% if var('is_delta_load') and var('delta_load_batch_name') != 'NA' and var('delta_load_batch_id') == -1 %}
        {{ log("Starting batch " ~ iag_common.mu_delta_load_get_batch_id(), info=True) }}
         {% do run_query("insert into " ~ var('delta_load_schema') ~ ".dbt_delta_load_master_control (sequence_id, batch_name, status, started_time) values (" ~ iag_common.mu_delta_load_get_batch_id() ~ ", '" ~ var('delta_load_batch_name') ~ "', '" ~ var('delta_load_started') ~ "', current_timestamp)") %}
    {% else %}
        {{ log("Not running on_run_start because the input parameters are incorrect", info=True) }}
    {% endif %}

{% endif %}

{%- endmacro %}
