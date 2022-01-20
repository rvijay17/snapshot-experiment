-----------------------------------------------------------------------------------------------------------------
-- Author      : Benoit Perigaud                                                      
-- Project Name: dbt enablement                                                           
-- Refresh EDH Dataset: N/A                                                                  
-- Description:                                                                          
--      New dbt "materialisation" that allows Views to not be dropped/recreated when the var non_destructive is True
--      This is based from the normal "view" materialisation, adding back code that was removed in this PR: https://github.com/dbt-labs/dbt/pull/1419
-- --------------------------------------------------------------------------------------------------------------
-- VERSIONS  DATE        WHO                DESCRIPTION                                   
-- 1.00      2021-10-29  Benoit Perigaud    Initial release
-- --------------------------------------------------------------------------------------------------------------*/

{%- materialization gp_view, default -%}

  {%- set identifier = model['alias'] -%}
  {%- set tmp_identifier = model['name'] + '__dbt_tmp' -%}
  {%- set backup_identifier = model['name'] + '__dbt_backup' -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier, schema=schema, database=database,
                                                type='view') -%}
  {%- set intermediate_relation = api.Relation.create(identifier=tmp_identifier,
                                                      schema=schema, database=database, type='view') -%}


    {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}
    {%- set has_transactional_hooks = (hooks | selectattr('transaction', 'equalto', True) | list | length) > 0 %}
    {%- set flag_non_destructive = var("non_destructive",False)  -%}
    {%- set should_ignore = flag_non_destructive and exists_as_view %}

  /*
     This relation (probably) doesn't exist yet. If it does exist, it's a leftover from
     a previous run, and we're going to try to drop it immediately. At the end of this
     materialization, we're going to rename the "old_relation" to this identifier,
     and then we're going to drop it. In order to make sure we run the correct one of:
       - drop view ...
       - drop table ...

     We need to set the type of this relation to be the type of the old_relation, if it exists,
     or else "view" as a sane default if it does not. Note that if the old_relation does not
     exist, then there is nothing to move out of the way and subsequentally drop. In that case,
     this relation will be effectively unused.
  */
  {%- set backup_relation_type = 'view' if old_relation is none else old_relation.type -%}
  {%- set backup_relation = api.Relation.create(identifier=backup_identifier,
                                                schema=schema, database=database,
                                                type=backup_relation_type) -%}

  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- drop the temp relations if they exists for some reason
  {{ adapter.drop_relation(intermediate_relation) }}

  {% if not should_ignore %}
    {{ drop_relation_if_exists(backup_relation) }}
  {% endif %}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% if should_ignore -%}
    {#
      -- Materializations need to a statement with name='main'.
      -- We could issue a no-op query here (like `select 1`), but that's wasteful. Instead:
      --   1) write the sql contents out to the compiled dirs
      --   2) return a status and result to the caller
    #}
    {% call noop_statement('main', 'Existing - Not touched', code='PASS') -%}
      -- Not running : non-destructive mode
      {{ sql }}
    {%- endcall %}
  {%- else -%}
    {% call statement('main') -%}
      {{ create_view_as(intermediate_relation, sql) }}
    {%- endcall %}
  {%- endif %}

  -- cleanup
  {% if not should_ignore -%}
    -- move the existing view out of the way
    {% if old_relation is not none %}
      {{ adapter.rename_relation(target_relation, backup_relation) }}
    {% endif %}
    {{ adapter.rename_relation(intermediate_relation, target_relation) }}
  {%- endif %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% if has_transactional_hooks or not should_ignore %}
      {{ adapter.commit() }}
  {% endif %}

  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization -%}