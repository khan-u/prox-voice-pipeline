-- Group 1+2: formation / glare / latency / uplink vs cluster size N.
-- Mirrors aggregate.py group12: setup uses a median, the rest a mean, over the
-- per-client rows of each N-run; negative sentinels (-1 = not measured) are
-- excluded before aggregating. Newest run per N wins — filenames share the
-- `wsn-voice-N<n>-summary-<ISO>` prefix, so the max source_file is the latest.
with all_runs as (
    select * from {{ ref('stg_voice_client') }}
),
latest as (
    select n_peers, max(source_file) as latest_file
    from all_runs
    group by n_peers
),
vc as (
    select all_runs.*
    from all_runs
    join latest
      on all_runs.n_peers = latest.n_peers
     and all_runs.source_file = latest.latest_file
)
select
    n_peers,
    round(avg(peers), 1)                                              as avg_peers,
    sum(greatest(links, 0))                                           as conn_links,
    round(percentile(case when setup_median_ms >= 0 then setup_median_ms end, 0.5)) as setup_median_ms,
    round(percentile(case when setup_p95_ms    >= 0 then setup_p95_ms    end, 0.5)) as setup_p95_ms,
    round(avg(case when ice_success_pct  >= 0 then ice_success_pct  end)) as ice_success_pct,
    round(avg(case when relay_pct        >= 0 then relay_pct        end)) as relay_pct,
    round(avg(case when lat_raw_median_ms >= 0 then lat_raw_median_ms end)) as m2e_ms,
    sum(glare_total)                                                  as glare_total,
    round(avg(mean_up_kbps))                                          as uplink_kbps,
    round(avg(suppressed_pct))                                        as suppressed_pct
from vc
group by n_peers
order by n_peers
