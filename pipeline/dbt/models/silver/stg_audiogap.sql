-- Per-run listener audio-gap timings; `valid` gates the mart to measured runs.
select
    source_file,
    valid,
    stop_ms,
    reconnect_ms,
    total_silence_ms,
    ingested_at
from delta.`{{ var('silver_root', 'lake/silver') }}/audiogap`
