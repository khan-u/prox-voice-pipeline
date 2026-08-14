-- Per-run mobility phases with the complete-case validity flag the mart filters.
select
    source_file,
    form_ms,
    teardown_ms,
    reconnect_ms,
    complete_case,
    ingested_at
from delta.`{{ var('silver_root', 'lake/silver') }}/mobility`
