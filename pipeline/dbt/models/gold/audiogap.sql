-- Group 3b: listener-perceived audio gap (out-and-back), valid runs only.
-- Mirrors aggregate.py audiogap: a run counts only when valid == true; the
-- flag is carried from silver so this mart just filters on it.
with a as (
    select * from {{ ref('stg_audiogap') }}
    where valid
)
select
    count(*)                                       as valid_runs,
    round(percentile(stop_ms, 0.5))               as stop_median_ms,
    min(stop_ms)                                  as stop_min_ms,
    max(stop_ms)                                  as stop_max_ms,
    round(percentile(reconnect_ms, 0.5))          as reconnect_median_ms,
    min(reconnect_ms)                             as reconnect_min_ms,
    max(reconnect_ms)                             as reconnect_max_ms,
    round(percentile(total_silence_ms, 0.5))      as total_silence_median_ms,
    min(total_silence_ms)                         as total_silence_min_ms,
    max(total_silence_ms)                         as total_silence_max_ms
from a
