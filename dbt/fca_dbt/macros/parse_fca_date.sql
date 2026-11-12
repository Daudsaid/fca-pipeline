{#- FCA dates arrive as text, usually DD/MM/YYYY; also accept YYYY-MM-DD / ISO timestamps.
    01/01/1901 is the FCA system's placeholder for "unknown" and becomes NULL. -#}
{% macro parse_fca_date(expr) -%}
    nullif(
        coalesce(
            try_strptime(left({{ expr }}, 10), '%d/%m/%Y')::date,
            try_cast(left({{ expr }}, 10) as date)
        ),
        date '1901-01-01'
    )
{%- endmacro %}
