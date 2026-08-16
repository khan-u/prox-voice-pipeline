-- ICE success is a percentage: any row outside [0, 100] is invalid.
select n_peers, ice_success_pct
from {{ ref('kpi_vs_n') }}
where ice_success_pct is not null
  and (ice_success_pct < 0 or ice_success_pct > 100)
