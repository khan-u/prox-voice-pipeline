-- Silver link-fact grain: one row per connected link (realnet) or per client
-- (voice runs), materialized by the Spark link-fact job.
select
    record_type,
    source_file,
    condition,
    session_id,
    n_peers,
    client,
    link_index,
    setup_ms,
    rtt_ms,
    candidate_type,
    lat_raw_ms,
    glare,
    ingested_at
from delta.`{{ var('silver_root', 'lake/silver') }}/link_fact`
