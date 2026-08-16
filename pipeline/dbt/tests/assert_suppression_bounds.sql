-- Suppression is a percentage: any scenario outside [0, 100] is invalid.
select scenario, suppression_median_pct
from {{ ref('sensing_suppression') }}
where suppression_median_pct < 0 or suppression_median_pct > 100
