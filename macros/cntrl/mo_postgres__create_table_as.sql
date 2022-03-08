-- ----------------------------------------------------------------------------------------------------
-- Author       : Unknown
-- Description  : Override the default postgres__create_table_as macro
--                To allow distribution key to be specific when creating table in greenplum.
--                by adding to the config e.g.
--                {{ config(materialized='table', distributed_by='columns_to_distrbuted') }}
-- ----------------------------------------------------------------------------------------------------
-- VERSIONS  DATE          WHO                      DESCRIPTION
-- 1.00                    Unknown                  Initial Release
-- 1.01      27/01/2022    Cindy Chow               Added iag_common to __with_table_storage_parameter
--                                                  and __distributed_by
-------------------------------------------------------------------------------------------------------

{% macro postgres__create_table_as(temporary, relation, sql) -%}
  {%- set raw_distributed_by = config.get('distributed_by', none) -%}
  {%- set raw_table_storage_parameter = config.get('table_storage_parameters', none) -%}

  create {% if temporary: -%}temporary{%- endif %} table
     {{ relation.include(database=(not temporary), schema=(not temporary)) }}
  {{ iag_common.__with_table_storage_parameter(raw_table_storage_parameter) }}
  as (
    {{ sql }}
  )
    {{ iag_common.__distributed_by(raw_distributed_by) }}
  ;
{% endmacro %}