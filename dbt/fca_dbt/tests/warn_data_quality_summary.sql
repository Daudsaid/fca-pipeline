-- Data-quality watchlist (warning, not failure): lists firms whose source data
-- still looks wrong after cleaning, so the numbers are visible on every build.
{{ config(severity='warn', warn_if='>500') }}
select frn, 'suspect status date' as issue from {{ ref('stg_fca_firms') }} where date_suspect
union all
select frn, 'invalid UK postcode' from {{ ref('stg_fca_firms') }} where postcode_valid = false
