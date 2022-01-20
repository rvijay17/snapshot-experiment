-----------------------------------------------------------------------------------------------------------------
-- Author      : Benoit Perigaud                                                      
-- Project Name: dbt enablement                                                           
-- Refresh EDH Dataset: N/A                                                                  
-- Description:                                                                          
--      New dbt "materialisation" that allows Tables to not be dropped/recreated when the var non_destructive is True
---     Instead the table is truncated and data is inserted
---     Details behind the design is available at this location: https://confluence.iag.com.au/display/CLD/dbt+design+patterns
--      This is based from the normal "table" materialisation, adding back code that was removed in this PR: https://github.com/dbt-labs/dbt/pull/1419
-- --------------------------------------------------------------------------------------------------------------
-- VERSIONS  DATE        WHO                DESCRIPTION                                   
-- 1.00      2021-10-29  Benoit Perigaud    Initial release
-- --------------------------------------------------------------------------------------------------------------*/


{% materialization gp_table, default %}
{# {{ log('Start of the materialisation', info=True) }} #}
  {%- set identifier = model['alias'] -%}
  {%- set tmp_identifier = model['name'] + '__dbt_tmp' -%}
  {%- set backup_identifier = model['name'] + '__dbt_backup' -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}

{# {{ log('Before checking if the table exists', info=True) }} #}
  {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}

  {%- set non_destructive_mode = var("non_destructive",False) -%}
  {%- set create_as_temporary = (exists_as_table and non_destructive_mode) -%}


  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

  {% if non_destructive_mode -%}
    {% set schema_intermediate_relation = target.schema ~ '_temp' %}
  {% else %}
    {% set schema_intermediate_relation = schema %}
  {% endif %}
  {%- set intermediate_relation = api.Relation.create(identifier=tmp_identifier,
                                                      schema=schema_intermediate_relation,
                                                      database=database,
                                                      type='table') -%}
  -- the intermediate_relation should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation
  {%- set preexisting_intermediate_relation = adapter.get_relation(identifier=tmp_identifier, 
                                                                   schema=schema_intermediate_relation,
                                                                   database=database) -%}

  {%- set preexisting_temp_schema_intermediate_relation = adapter.get_relation(identifier=tmp_identifier, 
                                                                   schema=target.schema ~ '_temp',
                                                                   database=database) -%}
  /*
      See ../view/view.sql for more information about this relation.
  */
  {# {{ log('Before setting up backup relationships', info=True) }} #}
  {%- set backup_relation_type = 'table' if old_relation is none else old_relation.type -%}
  {%- set backup_relation = api.Relation.create(identifier=backup_identifier,
                                                schema=schema ,
                                                database=database,
                                                type=backup_relation_type) -%}
  -- as above, the backup_relation should not already exist
  {%- set preexisting_backup_relation = adapter.get_relation(identifier=backup_identifier,
                                                             schema=schema,
                                                             database=database) -%}


  -- drop the temp relations if they exist already in the database
  {% if non_destructive_mode -%}
      -- noop
    {% else -%}
      {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
      {{ drop_relation_if_exists(preexisting_temp_schema_intermediate_relation) }}
  {%- endif %}
  
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  -- setup: if the target relation already exists, truncate or drop it (if it's a view)
  {% if non_destructive_mode -%}
    {% if exists_as_table -%}
      {{ adapter.truncate_relation(old_relation) }}
    {% elif exists_as_view -%}
      {{ adapter.drop_relation(old_relation) }}
      {%- set old_relation = none -%}
    {%- endif %}
  {%- endif %}

{# {{ log('After truncating/dropping the potentially existing table', info=True) }} #}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% call statement('main') -%}
    {%- if non_destructive_mode -%}
      {%- if old_relation is not none -%}
      {# {{ log('The final table already exists', info=True) }} #}

        {% if preexisting_intermediate_relation is not none %}
          {# {{ log('The table in the temp schema already exists', info=True) }} #}
          {% set dest_columns = adapter.get_columns_in_relation(old_relation) %}
          {% set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') %}
          insert into {{ intermediate_relation }} ({{ dest_cols_csv }}) (
            {{ sql }}
          );
        {% else %}
          {# {{ log("The table in the temp schema doesn't exist yet", info=True) }} #}
          {{ create_table_as(False, intermediate_relation, sql) }}
        {% endif %}

      {# {{ log("We insert from the temp table to  the final table", info=True) }} #}
        {% set dest_columns = adapter.get_columns_in_relation(old_relation) %}
        {% set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') %}

        insert into {{ target_relation }} ({{ dest_cols_csv }}) (
          select {{ dest_cols_csv }}
          from {{ intermediate_relation.include(database=(not False), schema=(not False)) }}
        );


      {%- else -%}
        {# {{ log("The final table doesn't exist yet", info=True) }} #}
        {{ create_table_as(create_as_temporary, target_relation, sql) }}
        {# {{ log("The final table has been created", info=True) }} #}
      {%- endif -%}
    {%- else -%}
      {# {{ log("Destructive mode is ON", info=True) }} #}
      {{ create_table_as(create_as_temporary, intermediate_relation, sql) }}
    {%- endif -%}
    {# {{ log('End of the main call', info=True) }} #}
  {%- endcall %}

  -- cleanup
  {% if non_destructive_mode -%}

    {% if preexisting_intermediate_relation is not none %}
      {# {{ log('Truncating ' ~ preexisting_intermediate_relation , info=True) }} #}
    {{ adapter.truncate_relation(preexisting_intermediate_relation) }}
    {% endif %}

    {% if preexisting_intermediate_relation is none and intermediate_relation is not none and old_relation is not none %}
      {# {{ log('Truncating ' ~ intermediate_relation , info=True) }} #}
    {{ adapter.truncate_relation(intermediate_relation) }}
    {% endif %}
    
  {%- else -%}
    {% if old_relation is not none %}
        {{ adapter.rename_relation(target_relation, backup_relation) }}
    {% endif %}
        {{ adapter.rename_relation(intermediate_relation, target_relation) }}
  {%- endif %}


{# {{ log('Before index creation', info=True) }} #}

  {% do create_indexes(target_relation) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% do persist_docs(target_relation, model) %}

{% if non_destructive_mode -%}
  
{%- endif -%}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  -- finally, drop the existing/backup relation after the commit
  {# {{ adapter.truncate_relation(intermediate_relation) }} #}
  {{ drop_relation_if_exists(backup_relation) }}



  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{# {{ log('End of the materialisation', info=True) }} #}
{% endmaterialization %}