-- ----------------------------------------------------------------------------------------------------
-- Author       : Vijay Rajagopalan (s709081)
-- Description  : In Delta Loading, we need to add extra metadata in every run. 
--                In the current implementation (v1.00), we are adding two extra columns.
--                - batch_id: The batch ID in which the current row was loaded into target
--                - row_status: Depending upon the retired column in source table, the value is generated.
--                  If the source table does not have a retired column, default to 'A'
--                  If the source table has a retired column and value is 0 or null, then 'A' else 'D'
-- ----------------------------------------------------------------------------------------------------
-- VERSIONS  DATE          WHO                      DESCRIPTION
-- 1.00                    Vijay Rajagopalan        Initial Release
-------------------------------------------------------------------------------------------------------

{% macro mu_delta_load_insert_metadata(src_schema_name, src_table_name, column_name) %}
    {% set select_query %}
        select count(*) from information_schema.columns where table_schema = '{{ src_schema_name }}' and table_name = '{{ src_table_name }}' and column_name = '{{ column_name }}'
    {% endset %}
    {% if execute %}
        {% set result = run_query(select_query) %}
        {% if result[0][0] == 1%}
            {% set retired_column %}
                case when coalesce({{ column_name }}, 0) = 0 then 'A' else 'D' end
            {% endset %}
        {% else %}
            {% set retired_column %}
                'A'
            {% endset %}    
        {% endif %}
        {{mu_delta_load_get_batch_id()}}::BIGINT as batch_id, {{ retired_column }}::TEXT as row_status
    {% endif %}
{% endmacro %}
