-- Real-network per-condition connection timing.
-- Mirrors aggregate.py realnet's per-connection high-res table: one row per
-- condition tag over its links, with setup and candidate-pair RTT medians.
-- access (cellular/home) and path (direct/relay) are derived so the figures can
-- contrast forced-relay against direct on each access network.
with lf as (
    select * from {{ ref('stg_link_fact') }}
    where record_type = 'realnet'
)
select
    condition,
    case
        when condition like 'cellular%' then 'cellular'
        when condition like 'homeNAT%'  then 'home'
        else 'other'
    end                                                          as access,
    case
        when sum(case when candidate_type = 'relay' then 1 else 0 end) > 0
        then 'relay' else 'direct'
    end                                                          as path,
    count(*)                                                     as links,
    round(percentile(setup_ms, 0.5))                            as setup_median_ms,
    round(percentile(case when rtt_ms >= 0 then rtt_ms end, 0.5)) as rtt_median_ms,
    sum(glare)                                                   as glare_total
from lf
group by condition
order by condition
