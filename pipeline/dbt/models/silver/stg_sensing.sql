-- Per-client sensor suppression by scenario, one row per client per run.
select
    source_file,
    scenario,
    client,
    suppressed_pct,
    ingested_at
from delta.`{{ var('silver_root', 'lake/silver') }}/sensing`
