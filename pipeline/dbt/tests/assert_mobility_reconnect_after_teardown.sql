-- Validity: a complete mobility cycle must have measured (non-negative) phase
-- medians. A negative median would mean a partial run leaked past the
-- complete-case filter.
select complete_cycles
from {{ ref('mobility_cycles') }}
where form_median_ms < 0
   or teardown_median_ms < 0
   or reconnect_median_ms < 0
