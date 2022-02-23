-- ----------------------------------------------------------------------------------------------------
-- Author       : Vijay Rajagopalan (s709081)
-- Description  : In Delta Loading, this macro is used to log the job results
-- ----------------------------------------------------------------------------------------------------
-- VERSIONS  DATE          WHO                      DESCRIPTION
-- 1.00                    Vijay Rajagopalan        Initial Release
-------------------------------------------------------------------------------------------------------

{% macro mu_delta_load_log_results(results) %}

  {% if execute %}
  {{ log("========== Begin Delta Load Summary ==========", info=True) }}
  {% for res in results -%}
    {% set line -%}
        node: {{ res.node.name }}; status: {{ res.status }} (message: {{ res.message }})
    {%- endset %}

    {{ log(line, info=True) }}
  {% endfor %}
  {{ log("========== End Delta Load Summary ==========", info=True) }}
  {% endif %}

{% endmacro %}
