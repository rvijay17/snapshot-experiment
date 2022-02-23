-- ----------------------------------------------------------------------------------------------------
-- Author       : Vijay Rajagopalan (s709081)
-- Description  : In Delta Loading, this macro is used to add entries to the dbt_delta_load_job_control and dbt_delta_load_master_control table 
--                For each job execution, there will be an entry in the table with status = 'success' or 'skipped' or 'error'.
--                If all the jobs have status = 'success', the batch is successful and therefore an entry with status = 'success' is added in dbt_delta_load_master_control.
--                If not all status = 'success', then an entry with status = 'error' is added in dbt_delta_load_master_control.
-- ----------------------------------------------------------------------------------------------------
-- VERSIONS  DATE          WHO                      DESCRIPTION
-- 1.00                    Vijay Rajagopalan        Initial Release
-- 1.01      2021-12-10    Long Vu                  Change delta_load_job_control to dbt_delta_load_job_control; Update column names;
-- 1.02      2021-12-16    Cindy Chow               Add iag_common prefix to macro name mu_delta_load_log_results
-------------------------------------------------------------------------------------------------------

{% macro mu_delta_load_master_control_complete() -%}

{# Scoping Behavior - It is not possible to set variables inside a block and have them show up outside of it. #}
{# https://jinja.palletsprojects.com/en/3.0.x/templates/#assignments #}

{% if execute %}

    {{ iag_common.mu_delta_load_log_results(results) }}

    {% if var('is_delta_load') and var('delta_load_batch_name') != 'NA' %}
        {# set batchFailed = False #}
        {% set batch_id = iag_common.mu_delta_load_get_batch_id() if var('delta_load_batch_id') == -1 else var('delta_load_batch_id') %}
        {% for res in results -%}
            {# Since we are not re-starting a job, there is no need to check control tables #}
            {# if mu_delta_load_run_snapshot_yn(res.node.name) #}
                {% do run_query("insert into " ~ var('delta_load_schema') ~ ".dbt_delta_load_job_control (batch_seq_id, target_table_name, job_status, job_start_timestamp) 
                    values (" ~ batch_id ~ ", '" ~ res.node.name ~ "', '" ~ res.status ~ "', current_timestamp)") %}
            {# endif #}
            {# Not able to use the following code because of the scoping behaviour. Using Select count(*) instead. #}
            {# if not batchFailed and res.status != 'success' %}
                {% set batchFailed = True %}
            {% endif #}
        {%- endfor %}

        {% set jobFailed = run_query("select count(*) from " ~ var('delta_load_schema') ~ ".dbt_delta_load_job_control where batch_seq_id = " ~ batch_id ~ " and job_status = '" ~ var('delta_load_error') ~ "'") %}

        {% if jobFailed|length > 0 and jobFailed[0][0] > 0 %}
            {% do run_query("insert into " ~ var('delta_load_schema') ~ ".dbt_delta_load_master_control (sequence_id, batch_name, status, started_time) 
                values (" ~ batch_id ~ ", '" ~ var('delta_load_batch_name') ~ "', '" ~ var('delta_load_error') ~ "', current_timestamp)") %}
        {% else %}
            {% do run_query("insert into " ~ var('delta_load_schema') ~ ".dbt_delta_load_master_control (sequence_id, batch_name, status, started_time) 
                values (" ~ batch_id ~ ", '" ~ var('delta_load_batch_name') ~ "', '" ~ var('delta_load_success') ~ "', current_timestamp)") %}
        {% endif %}
    {% else %}
        {{ log("Not running on_run_end becuase the input parameters are incorrect", info=True) }}
    {% endif %}

{% endif %}

{%- endmacro %}
