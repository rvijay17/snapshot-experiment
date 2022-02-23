-- ----------------------------------------------------------------------------------------------------
-- Author       : Vijay Rajagopalan (s709081)
-- Description  : In Delta Loading, this macro is used to add entries to the dbt_delta_load_job_control table 
--                For each job (or sql file in DBT), there will be an entry in the table with status = 'started'.
--                In the current implementation, delta_load_batch_id is always = -1.
--                So, we never pass a value for delta_load_batch_id when we run dbt snapshot, which means that
--                a batch is never restarted. If a batch fails, a new batch is started which will have a new Batch ID
-- ----------------------------------------------------------------------------------------------------
-- VERSIONS  DATE          WHO                      DESCRIPTION
-- 1.00                    Vijay Rajagopalan        Initial Release
-- 1.01      2021-12-10    Long Vu                  Change delta_load_job_control to dbt_delta_load_job_control; Update column names;
-------------------------------------------------------------------------------------------------------

{% macro mu_delta_load_job_control_start(table_name) -%} 

{% if execute %}

    {# Check if the current run is a delta load #}
    {% if var('is_delta_load') and var('delta_load_batch_name') != 'NA' %}
        {# Check whether delta load job id was passed as a parameter to dbt snapshot #}
        {% if var('delta_load_batch_id') == -1 %}
            {% do run_query("insert into " ~ var('delta_load_schema')  ~ ".dbt_delta_load_job_control (batch_seq_id, target_table_name, job_status, job_start_timestamp) values (" ~ mu_delta_load_get_batch_id() ~ ", '" ~ table_name ~ "', '" ~ var('delta_load_started') ~ "', current_timestamp)") %}
        {% endif %}
        {# In v1.00 of the implementation, we'll no be passing a Batch ID as a parameter, so it'll be -1 always #}
        {# In future implementations, we might consider re-starting a batch by passing a Batch ID #}
        {# In which case, consider adding the folowing piece of code in the else section of the if statement #}
        {# Find whether job has started but not completed #}
        {# If job has completed, do not run again #}
        {# if mu_delta_load_run_snapshot_yn(table_name) #}
            {# do run_query("insert into " ~ var('delta_load_schema') ~ ".dbt_delta_load_job_control (batch_seq_id, target_table_name, job_status, job_start_timestamp) values (" ~ var('delta_load_batch_id') ~ ", '" ~ table_name ~ "', '" ~ var('delta_load_started') ~ "', current_timestamp)") #}
        {# endif #}
    {% else %}
        {{ log("Not a delta load", info=True) }}
    {% endif %}

{% endif %}

{%- endmacro %}
