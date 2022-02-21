{% macro mu_insert_batch_id() %}
    {% set select_query %}
        select count(*) from information_schema.columns where table_name = 'orders' and column_name = 'retired'
    {% endset %}
    {% set result = run_query(select_query) %}
    {% if result[0][0] == 1%}
        {% set retired_column %}
            case when nullif(retired, 0) != 0 then 'D' else 'A' end
        {% endset %}
    {% else %}
        {% set retired_column %}
            'A'
        {% endset %}    
    {% endif %}
    {{run_started_at.strftime("%s")}} as batch_id, {{ retired_column }} as row_status, 
{% endmacro %}