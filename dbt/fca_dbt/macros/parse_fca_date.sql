{#- FCA dates arrive as text, usually DD/MM/YYYY; also accept YYYY-MM-DD / ISO timestamps. -#}
{% macro parse_fca_date(expr) -%}
    coalesce(
        try_strptime(left({{ expr }}, 10), '%d/%m/%Y')::date,
        try_cast(left({{ expr }}, 10) as date)
    )
{%- endmacro %}
