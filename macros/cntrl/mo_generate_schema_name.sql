-- ----------------------------------------------------------------------------------------------------
-- Author       : Benoit Perigaud
-- Description  : Override the default generate_schema_name macro
--                To allow the creation of tables in schemas pub, ctx etc... and avoid
--                concatenating the schema from the profile with the schema of the folder
-- ----------------------------------------------------------------------------------------------------
-- VERSIONS  DATE          WHO                      DESCRIPTION
-- 1.00      19/08/2021    Benoit Perigaud          Initial Release
-------------------------------------------------------------------------------------------------------

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if default_schema == 'no_prefix' -%}
          {{ custom_schema_name | trim or 'dbt'}}

    {%- else -%}

      {%- if custom_schema_name is none -%}
          {{ default_schema }}
      {%- else -%}
          {{ default_schema }}_{{ custom_schema_name | trim }}
      {%- endif -%}

    {%- endif -%}

{%- endmacro %}