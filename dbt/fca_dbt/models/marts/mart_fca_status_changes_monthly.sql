/*
  Monthly trend of status transitions (e.g. authorised -> no longer authorised),
  bucketed by the FCA's effective date, falling back to when we detected it.
*/
select
    date_trunc('month', coalesce(effective_date, detected_at::date))::date  as change_month,
    status_before,
    status_after,
    count(*)                                                                as num_firms
from {{ ref('int_fca_status_changes') }}
group by 1, 2, 3
order by 1 desc, 4 desc
