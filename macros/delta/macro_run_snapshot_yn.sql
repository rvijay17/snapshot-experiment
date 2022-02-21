{% macro macro_run_snapshot_yn(table_name) -%} 

{% set runSnapshot = False %}

{% if execute %}
    {% if var('delta_load_batch_id') != -1 %}
        {% set hasJobRunBefore = run_query("select count(*) from " ~ var("delta_load_schema") ~ ".delta_load_job_control where batch_seq_id = " ~ var('delta_load_batch_id') ~ " and job_table_name = '" ~ table_name ~ "' and job_table_status = '" ~ var('success_status') ~ "' ") %}
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