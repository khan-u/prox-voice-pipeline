-- Sensing layer: report-suppression by scenario.
-- Mirrors aggregate.py sensing: pool every client's suppressedPct within a
-- scenario and take the median; trials is the number of runs in that scenario.
with s as (
    select * from {{ ref('stg_sensing') }}
)
select
    scenario,
    count(distinct source_file)              as trials,
    round(percentile(suppressed_pct, 0.5))   as suppression_median_pct
from s
group by scenario
order by scenario
