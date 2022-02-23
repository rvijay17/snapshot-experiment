{% macro delta_load_master_control_complete() -%}

{# Scoping Behavior - It is not possible to set variables inside a block and have them show up outside of it. #}
{# https://jinja.palletsprojects.com/en/3.0.x/templates/#assignments #}

{% if execute %}

    {{ log_results(results) }}

    {% if var('is_delta_load') and var('batch_name') != 'NA' %}
        {% set batchFailed = False %}
        {% set batch_id = run_started_at.strftime("%s") if var('delta_load_batch_id') == -1 else var('delta_load_batch_id') %}
        {% for res in results -%}
            {% if macro_run_snapshot_yn(res.node.name) %}
                {% do run_query("insert into " ~ var("delta_load_schema") ~ ".delta_load_job_control (batch_seq_id, job_table_name, job_table_status, table_load_start_time) values (" ~ batch_id ~ ", '" ~ res.node.name ~ "', '" ~ res.status ~ "', current_timestamp)") %}
            {% endif %}
            {# NOt able to use the following code becuase of the scoping behaviour. Using Select count(*) instead. #}
            {# if not batchFailed and res.status != 'success' %}
                {% set batchFailed = True %}
                {{ log("batchFailed ----- " ~ batchFailed, info=True)}}
            {% endif #}
        {%- endfor %}

        {% set jobFailed = run_query("select count(*) from " ~ var("delta_load_schema") ~ ".delta_load_job_control where batch_seq_id = " ~ batch_id ~ " and job_table_status = '" ~ var('error_status') ~ "'") %}

        {% if jobFailed|length > 0 and jobFailed[0][0] > 0 %}
            {% do run_query("insert into " ~ var("delta_load_schema") ~ ".delta_load_master_control (sequence_id, batch_name, status, started_time) values (" ~ batch_id ~ ", '" ~ var('batch_name') ~ "', '" ~ var('error_status') ~ "', current_timestamp)") %}
        {% else %}
            {% do run_query("insert into " ~ var("delta_load_schema") ~ ".delta_load_master_control (sequence_id, batch_name, status, started_time) values (" ~ batch_id ~ ", '" ~ var('batch_name') ~ "', '" ~ var('success_status') ~ "', current_timestamp)") %}
        {% endif %}
    {% else %}
        {{ log("Not running on_run_start becuase the input parameters are incorrect", info=True) }}
    {% endif %}

{% endif %}

{%- endmacro %}