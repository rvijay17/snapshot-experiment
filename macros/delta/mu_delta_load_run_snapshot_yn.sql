-- ----------------------------------------------------------------------------------------------------
-- Author       : Vijay Rajagopalan (s709081)
-- Description  : In Delta Loading, this macro is used to check whether a batch was successfully run or not.
--                This macro is NOT used in the v1.00 implementation of Delta Loading.
--                In the current implementation, delta_load_batch_id is always = -1.
--                So, we never pass a value for delta_load_batch_id when we run dbt snapshot, which means that
--                a batch is never restarted. If a batch fails, a new batch is started which will have a new Batch ID
-- ----------------------------------------------------------------------------------------------------
-- VERSIONS  DATE          WHO                      DESCRIPTION
-- 1.00                    Vijay Rajagopalan        Initial Release
-- 1.01      2021-12-10    Long Vu                  Change delta_load_job_control to dbt_delta_load_job_control; Update column names;
-------------------------------------------------------------------------------------------------------

{% macro mu_delta_load_run_snapshot_yn(table_name) -%} 

{% set runSnapshot = False %}

{% if execute %}
    {% if var('delta_load_batch_id') != -1 %}
        {% set hasJobRunBefore = run_query("select count(*) from " ~ target.schema ~ ".dbt_delta_load_job_control where batch_seq_id = " ~ var('delta_load_batch_id') ~ " and job_table_name = '" ~ table_name ~ "' and job_status = '" ~ var('delta_load_success') ~ "' ") %}
        {% if hasJobRunBefore[0][0] == 0 %}
            {# This job has not been completed before, so running again #}
            {% set runSnapshot = True %}
        {% endif %}
    {% else %}
        {# Snapshot id not passed #}
        {% set runSnapshot = True %}
    {% endif %}
{% endif %}

{{ return(runSnapshot) }}

{% endmacro %}
