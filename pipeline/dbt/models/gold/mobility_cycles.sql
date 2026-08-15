-- Group 3: mobility teardown / reconnect timing, complete-case only.
-- Mirrors aggregate.py group3: a cycle counts only when all three phases
-- completed; partial runs are dropped upstream via the complete_case flag.
-- One summary row of medians and observed ranges.
with m as (
    select * from {{ ref('stg_mobility') }}
    where complete_case
)
select
    count(*)                                 as complete_cycles,
    round(percentile(form_ms, 0.5))          as form_median_ms,
    min(form_ms)                             as form_min_ms,
    max(form_ms)                             as form_max_ms,
    round(percentile(teardown_ms, 0.5))      as teardown_median_ms,
    min(teardown_ms)                         as teardown_min_ms,
    max(teardown_ms)                         as teardown_max_ms,
    round(percentile(reconnect_ms, 0.5))     as reconnect_median_ms,
    min(reconnect_ms)                        as reconnect_min_ms,
    max(reconnect_ms)                        as reconnect_max_ms
from m
