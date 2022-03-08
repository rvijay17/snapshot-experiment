-- ----------------------------------------------------------------------------------------------------
-- VERSIONS  DATE          WHO               DESCRIPTION
-- 1.00      20/01/2022    Cindy Chow        Initial version
-------------------------------------------------------------------------------------------------------

{% macro mu_alter_columns_set_not_null(column_names) %}

    {%- set select_query -%}
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{{this.schema}}'
        AND table_name = '{{this.table}}'
        AND column_name in ({%- for col in column_names %}
                                '{{col}}'
                            {%- if not loop.last %},{%- endif %}
                            {%- endfor %})
        AND is_nullable = 'YES'
        ;
    {%- endset -%}

    {% if execute %}

    {%- set result = run_query(select_query) -%}

    {%- for col in result -%}

    {%- if loop.first -%}
    ALTER TABLE {{this}}
    {%- endif %}
    ALTER COLUMN {{col[0]}} SET NOT NULL
    {%- if not loop.last -%},{% endif %}
    {%- endfor -%}
    ;

    {%- endif -%}

{% endmacro %}
