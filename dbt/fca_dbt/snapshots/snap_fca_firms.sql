{#-
  SCD Type 2 history of each firm. A new row is opened whenever one of the
  check_cols changes between daily runs.
  hard deletes are NOT invalidated: the free API is sampled via search, so a
  firm missing from one run does not mean it left the Register.
-#}
{% snapshot snap_fca_firms %}

{{
    config(
        target_schema='snapshots',
        unique_key='frn',
        strategy='check',
        check_cols=['firm_name', 'status', 'sub_status', 'business_type', 'postcode'],
    )
}}

select * from {{ ref('stg_fca_firms') }}

{% endsnapshot %}
