-- ----------------------------------------------------------------------------------------------------
-- Author       : Benoit Perigaud
-- Description  : Provides relevant SQL access to objects once created via dbt 
--                The name of the role is a combination of the database, schema and a suffix
-- ----------------------------------------------------------------------------------------------------
-- VERSIONS  DATE          WHO                      DESCRIPTION
-- 1.00      2021-08-16    Benoit Perigaud          Initial Release
-- 1.01      2021-09-06    Benoit Perigaud          Modify the macro to only run on non-dev
-- 1.02      2021-09-07    Benoit Perigaud          Modify the macro to exclude the deploy_vm targets
-- 1.03      2021-09-09    Benoit Perigaud          Modify the macro to cater for all use cases
-- 1.04      2021-09-14    Benoit Perigaud          Change the owner role for sit and uat to fix permission
-- 1.05      2021-09-14    Benoit Perigaud          Change owner for targets to new groups
-- 1.06      2021-12-13    Ruoran Huang             Added mu_grant_dependency_schema_access macro
-- 1.07      2021-12-14    Ruoran Huang             Added ci schema to macro
-- 1.08      2021-12-14    Ruoran Huang             Removed mu_grant_dependency_schema_access macro
-------------------------------------------------------------------------------------------------------

{% macro mu_grant_schema_access(this) %}

{% if target.name in ['ci','sit','uat'] %} {# For project databases #}
GRANT SELECT ON {{ this }} TO {{ target.dbname }}_{{ this.schema }}_read_role;
GRANT ALL ON {{ this }} TO {{ target.dbname }}_{{ this.schema }}_crt_role;
GRANT ALL ON {{ this }} TO {{ target.dbname }}_{{ this.schema }}_wrt_role;
ALTER TABLE {{ this }} OWNER TO uproj_new_dbt_sys_users;
{% endif %}

{% if target.name in ['prod'] %} {# For production database #}
GRANT ALL ON {{ this }} TO {{ this.schema }}_wrt_role;
GRANT SELECT ON {{ this }} TO {{ this.schema }}_read_role;
ALTER TABLE {{ this }} OWNER TO ugrp_dbt;
{% endif %}

{% if target.name in ['prod_test'] %} {# For CICD production testing database #}
GRANT ALL ON {{ this }} TO {{ this.schema }}_wrt_role;
GRANT SELECT ON {{ this }} TO {{ this.schema }}_read_role;
ALTER TABLE {{ this }} OWNER TO uproj_new_dbt_sys_users;
{% endif %}

{% if target.name in ['preprod','preprod_test'] %} {# For preprod and CICD preprod testing database #}
GRANT ALL ON {{ this }} TO preprod_{{ this.schema }}_wrt_role;
GRANT SELECT ON {{ this }} TO preprod_{{ this.schema }}_read_role;
ALTER TABLE {{ this }} OWNER TO uproj_new_dbt_sys_users;
{% endif %}

{% endmacro %}