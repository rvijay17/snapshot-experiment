-- ----------------------------------------------------------------------------------------------------
-- Author       : Benoit Periguad
-- Description  : A macro used to set session parameters while running specific moodels
-- ----------------------------------------------------------------------------------------------------
-- VERSIONS  DATE          WHO                      DESCRIPTION
-- 1.00      20/10/2021    Benoit Perigaud          Allow users to set session parameters (like set optimizer='off') when running models
-------------------------------------------------------------------------------------------------------


{% macro mu_set_gp_session_parameter(session_parameter_string) %}
{# use the macro like {{ iag_common.mu_set_gp_session_parameter("set gp_max_slices=250;") }} #}

{% if execute %}
  {% set query = session_parameter_string %}
   {% do run_query(query) %}
{% endif %}

{% endmacro %}