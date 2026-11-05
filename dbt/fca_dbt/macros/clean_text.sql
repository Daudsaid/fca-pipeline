{#- Trim text and turn empty strings and the API's 'NA' placeholder into NULL. -#}
{% macro clean_text(expr) -%}
    nullif(nullif(trim({{ expr }}), ''), 'NA')
{%- endmacro %}
