{% macro delta_load_job_control_start(table_name) -%} 

{% if execute %}

    {# Check if the current run is a delta load #}
    {% if var('is_delta_load') and var('batch_name') != 'NA' %}
        {# Check if a delta load job id was passed as a parameter to dbt snapshot #}
        {% if var('delta_load_batch_id') == -1 %}
            {% do run_query("insert into " ~ var("delta_load_schema") ~ ".delta_load_job_control (batch_seq_id, job_table_name, job_table_status, table_load_start_time) values (" ~ run_started_at.strftime("%s") ~ ", '" ~ table_name ~ "', '" ~ var('started_status') ~ "', current_timestamp)") %}
        {% else %}
            {# delta load batch id was passed #}
            {# Find whether job has started but not completed ##}
            {# If job has completed, do not run again ##}
            {% if macro_run_snapshot_yn(table_name) %}
                {% do run_query("insert into " ~ var("delta_load_schema") ~ ".delta_load_job_control (batch_seq_id, job_table_name, job_table_status, table_load_start_time) values (" ~ var('delta_load_batch_id') ~ ", '" ~ table_name ~ "', '" ~ var('started_status') ~ "', current_timestamp)") %}
            {% endif %}            
        {% endif %}
    {% else %}
        {{ log("Not a delta load", info=True) }}
    {% endif %}

{% endif %}

{%- endmacro %}