/*
  Current state of every firm (open snapshot rows only).
*/
select
    frn,
    firm_name,
    status,
    status_effective_date,
    sub_status,
    business_type,
    companies_house_number,
    town,
    postcode,
    country,
    website,
    dbt_valid_from as current_since
from {{ ref('snap_fca_firms') }}
where dbt_valid_to is null
