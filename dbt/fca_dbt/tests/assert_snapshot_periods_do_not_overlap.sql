-- Fails if any firm has two snapshot rows whose validity windows overlap.
select a.frn
from {{ ref('snap_fca_firms') }} a
join {{ ref('snap_fca_firms') }} b
  on a.frn = b.frn
 and a.dbt_scd_id <> b.dbt_scd_id
 and a.dbt_valid_from < coalesce(b.dbt_valid_to, 'infinity'::timestamp)
 and b.dbt_valid_from < coalesce(a.dbt_valid_to, 'infinity'::timestamp)
