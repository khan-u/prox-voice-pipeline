-- Per-run DTX uplink, keyed by source (tone/speech) and DTX on/off.
select
    source_file,
    source,
    dtx,
    uplink_kbps,
    ingested_at
from delta.`{{ var('silver_root', 'lake/silver') }}/dtx`
