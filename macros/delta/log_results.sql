{% macro log_results(results) %}

  {% if execute %}
  {{ log("========== Begin Summary ==========", info=True) }}
  {% for res in results -%}
    {% set line -%}
        node: {{ res.node.name }}; status: {{ res.status }} (message: {{ res.message }})
    {%- endset %}

    {{ log(line, info=True) }}
  {% endfor %}
  {{ log("========== End Summary ==========", info=True) }}
  {% endif %}

{% endmacro %}