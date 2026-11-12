/*
  Current state of every firm.
  Attributes come from staging (so the latest cleaning rules always apply to every
  firm); current_since comes from the open SCD2 snapshot row (when the firm last
  changed in a tracked attribute).
*/
select
    s.frn,
    s.firm_name,
    s.status,
    s.status_group,
    s.status_effective_date,
    s.date_suspect,
    s.sub_status,
    s.business_type,
    s.companies_house_number,
    s.town,
    s.town_inferred,
    s.postcode,
    s.postcode_valid,
    s.country,
    s.website,
    snap.dbt_valid_from as current_since
from {{ ref('stg_fca_firms') }} s
join {{ ref('snap_fca_firms') }} snap
  on snap.frn = s.frn
 and snap.dbt_valid_to is null
