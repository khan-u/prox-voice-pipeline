-- DTX uplink: media-plane event-driven transmission by source and DTX on/off.
-- Mirrors aggregate.py dtx: group by (source, dtx), median uplink over runs with
-- a measured uplink.
with d as (
    select * from {{ ref('stg_dtx') }}
    where uplink_kbps >= 0
)
select
    source,
    dtx,
    count(*)                                as n,
    round(percentile(uplink_kbps, 0.5))     as uplink_median_kbps
from d
group by source, dtx
order by source, dtx
