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
{% macro __distributed_by(raw_distributed_by) %}
  {%- if raw_distributed_by is none -%}
    {{ return('') }}
  {%- endif -%}

  {%- if raw_distributed_by in ['randomly', 'RANDOMLY'] -%}
    {% set distributed_by_clause %}
      distributed randomly
    {%- endset -%}
  {%- else -%}  
    {% set distributed_by_clause %}
      distributed by ({{ raw_distributed_by }})
    {%- endset -%}
  {%- endif -%}

  {{ return(distributed_by_clause) }}
{%- endmacro -%}