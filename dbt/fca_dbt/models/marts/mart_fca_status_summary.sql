/*
  How many firms are in each status right now.
*/
select
    status,
    count(*)                                                    as num_firms,
    round(100.0 * count(*) / sum(count(*)) over (), 1)          as pct_of_firms
from {{ ref('mart_fca_firms_current') }}
group by status
order by num_firms desc
