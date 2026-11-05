/*
  One row per status change seen in the snapshot history.
    detected_at     - when our pipeline first saw the change
    effective_date  - the date the FCA says the change took effect
*/
with history as (
    select
        frn,
        firm_name,
        status,
        status_effective_date,
        dbt_valid_from,
        lag(status) over (partition by frn order by dbt_valid_from) as previous_status
    from {{ ref('snap_fca_firms') }}
)

select
    frn,
    firm_name,
    previous_status         as status_before,
    status                  as status_after,
    status_effective_date   as effective_date,
    dbt_valid_from          as detected_at
from history
where previous_status is not null
  and previous_status is distinct from status
