-- ----------------------------------------------------------------------------------------------------
-- Author       : Benoit Perigaud
-- Description  : Override the default postgres__create_schema macro
--                To allow authorised SQL users to create schemas in GP 
-- ----------------------------------------------------------------------------------------------------
-- VERSIONS  DATE          WHO                      DESCRIPTION
-- 1.00      2021-08-10    Benoit Perigaud          Initial Release
-------------------------------------------------------------------------------------------------------

{% macro postgres__create_schema(relation) -%}
  {% if relation.database -%}
    {{ adapter.verify_database(relation.database) }}
  {%- endif -%}
  {%- call statement('create_schema') -%}
    create schema {{ relation.without_identifier().include(database=False) }}
  {%- endcall -%}
{% endmacro %}