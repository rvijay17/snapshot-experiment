-- ----------------------------------------------------------------------------------------------------
-- Author       : Ruoran Huang
-- Description  : Grant access to all dependency schemas outside of dbt
-- 
-- ----------------------------------------------------------------------------------------------------
-- VERSIONS  DATE          WHO               DESCRIPTION
-- 1.00      2021-12-13    Ruoran Huang      DBT-1  : Initial version
-- 1.01      2021-12-14    Ruoran Huang      Added 'if execute' to macro so it won't trigger during parse
-- 1.02      2021-12-14    Ruoran Huang      modified macro to execute during on-run-end
-- 1.03      2022-02-01    Long Vu           Apply GRANT statements to CI, SIT and UAT targets only
-------------------------------------------------------------------------------------------------------


{% macro mu_grant_dependency_schema_access(schemas) -%}


{% if execute %}
    {# generate a list of all sources #}
    {% set sources = [] -%}
    {% for node in graph.sources.values() -%}
        {%- do sources.append(source(node.source_name, node.name)) -%}
    {%- endfor %}

    {# convert sources to string and remove table name #}
    {% set all_source_db_schemas = [] -%}
    {%- for source in sources -%}
        {%- set source = source | string | replace('"','') -%}
        {%- do all_source_db_schemas.append(source.split('.')[0:2]) -%}
    {%- endfor %}

    {# make a unique list of database and schema names #}
    {%- set unique_source_db_schemas = [] -%}
    {%- for db_schema in all_source_db_schemas if db_schema not in unique_source_db_schemas -%}
        {%- do unique_source_db_schemas.append(db_schema) -%}
    {%- endfor %}

    {# return grant statement/s #}
    {%- for source in unique_source_db_schemas %}
        {%- for schema in schemas -%}

            {% if target.name in ['ci','sit','uat'] %} {# For project databases #}
        GRANT {{source[0]}}_{{source[1]}}_read_role TO {{ target.dbname }}_{{ schema }}_crt_role;
            {% endif %}

        {% endfor %}
    {% endfor %}
{% endif %}

{% endmacro %}