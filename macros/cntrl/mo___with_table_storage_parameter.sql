-- ----------------------------------------------------------------------------------------------------
-- Author       : Unknown
-- Description  : Override the default postgres__create_table_as macro
--                To allow distribution key to be specific when creating table in greenplum.
--                by adding to the config e.g.
--                {{ config(materialized='table', distributed_by='columns_to_distrbuted') }}
-- ----------------------------------------------------------------------------------------------------
-- VERSIONS  DATE          WHO                      DESCRIPTION
-- 1.00                    Unknown                  Initial Release
-------------------------------------------------------------------------------------------------------


-- this is used internally --
{% macro __with_table_storage_parameter(raw_table_storage_parameter) %}
  {%- if raw_table_storage_parameter is none -%}
    {{ return('') }}
  {%- endif -%}

  {% set table_storage_parameters %}
  with (
  {% if raw_table_storage_parameter is string -%}
    {% set table_storage_parameters = [raw_table_storage_parameter] %}
  {%- else -%}
    {%- for param in raw_table_storage_parameter -%}
      {{ param }}
      {%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
  {%- endif -%}
  )
  {%- endset -%}

  {{ return(table_storage_parameters)}}

{%- endmacro -%}
