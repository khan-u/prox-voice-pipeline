-- Per-run grace-window duty-cycle measurements, keyed by (absence, grace).
select
    source_file,
    absence_ms,
    grace_ms,
    reconnect_gap_ms,
    hold_kbps,
    stop_ms,
    ingested_at
from delta.`{{ var('silver_root', 'lake/silver') }}/gracegap`
