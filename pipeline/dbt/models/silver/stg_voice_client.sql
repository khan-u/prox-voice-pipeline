-- Rich per-client voice KPIs (ICE%, relay%, uplink, suppression) for the vs-N
-- marts, one row per client per N-run.
select
    source_file,
    n_peers,
    client,
    peers,
    links,
    setup_median_ms,
    setup_p95_ms,
    ice_success_pct,
    relay_pct,
    lat_raw_median_ms,
    glare_total,
    mean_up_kbps,
    suppressed_pct,
    ingested_at
from delta.`{{ var('silver_root', 'lake/silver') }}/voice_client`
