{#- Trim text, collapse repeated whitespace, and turn empty strings and the API's
    placeholder values ('NA', '-', 'N/A') into NULL. -#}
{% macro clean_text(expr) -%}
    nullif(nullif(nullif(nullif(
        regexp_replace(trim({{ expr }}), '\s+', ' ', 'g'),
    ''), 'NA'), '-'), 'N/A')
{%- endmacro %}

{#- Title-case a place name: 'LONDON' / 'london,' -> 'London'. -#}
{% macro title_case_place(expr) -%}
    nullif(
        array_to_string(
            list_transform(
                string_split(lower(regexp_replace({{ expr }}, '[\s,.;:]+$', '')), ' '),
                lambda w: upper(w[1]) || w[2:]
            ),
            ' '
        ),
    '')
{%- endmacro %}
