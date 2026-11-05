-- Fails if a firm has more than one open (current) snapshot row.
select frn
from {{ ref('snap_fca_firms') }}
where dbt_valid_to is null
group by frn
having count(*) > 1
