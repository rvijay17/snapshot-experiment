{% macro delta_load_master_control_start() -%}

{% if execute %}

    {% if var('is_delta_load') and var('batch_name') != 'NA' and var('delta_load_batch_id') == -1 %}
        {{ log("Starting batch " ~ run_started_at.strftime("%s") , info=True) }}
        {% do run_query("insert into " ~ var("delta_load_schema") ~ ".delta_load_master_control (sequence_id, batch_name, status, started_time) values (" ~ run_started_at.strftime("%s") ~ ", '" ~ var('batch_name') ~ "', '" ~ var('started_status') ~ "', current_timestamp)") %}
    {% else %}
        {{ log("Not running on_run_start becuase the input parameters are incorrect", info=True) }}
    {% endif %}

{% endif %}

{%- endmacro %}