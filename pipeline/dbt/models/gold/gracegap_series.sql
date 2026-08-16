-- Grace-window duty-cycle sweep: reconnect gap vs grace window, by absence.
-- Mirrors aggregate.py gracegap: group by (absence, grace); a run counts only
-- when its reconnect gap was measured (reconnect_gap_ms >= 0). The D=6 s sweep
-- and D=12 s knee validation stay separate cells via the absence key.
with g as (
    select * from {{ ref('stg_gracegap') }}
    where reconnect_gap_ms >= 0
)
select
    absence_ms,
    grace_ms,
    count(*)                                                        as n,
    round(percentile(reconnect_gap_ms, 0.5))                       as gap_median_ms,
    min(reconnect_gap_ms)                                          as gap_min_ms,
    max(reconnect_gap_ms)                                          as gap_max_ms,
    round(percentile(case when stop_ms >= 0 then stop_ms end, 0.5)) as stop_median_ms,
    round(percentile(case when hold_kbps >= 0 then hold_kbps end, 0.5)) as hold_median_kbps
from g
group by absence_ms, grace_ms
order by absence_ms, grace_ms
