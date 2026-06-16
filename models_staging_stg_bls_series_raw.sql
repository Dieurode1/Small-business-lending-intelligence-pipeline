{{
    config(
        materialized='view'
    )
}}

-- ============================================================================
-- stg_bls_series_raw
-- ============================================================================
-- Flattens the BLS RAW VARIANT into one row per (series, year, period).
-- Source: bls_series_raw.raw_response:data (pre-unwrapped by bls.py).
--
-- Handles BLS-specific gotchas:
--   - value is a STRING in the source; '-' is the sentinel for missing data
--     (NULL-ed here, not cast directly or the FLOAT conversion would error)
--   - period 'M13' = annual average; filtered out — staging is monthly grain
--   - preliminary flag lives at footnotes[0].code = 'P'; surfaced as boolean
--   - period 'MQQ' (quarterly) doesn't appear for the 6 series we ingest,
--     but the WHERE clause is explicit so it wouldn't sneak in if added later
-- ============================================================================

with source as (
    select
        series_id,
        pulled_date,
        raw_response
    from {{ source('raw', 'bls_series_raw') }}
),

flattened as (
    select
        s.series_id,
        s.pulled_date,
        obs.value:year::integer        as observation_year,
        obs.value:period::string       as observation_period,
        obs.value:periodName::string   as period_name,
        obs.value:value::string        as observation_value_raw,
        obs.value:footnotes            as footnotes
    from source s,
         lateral flatten(input => s.raw_response:data) obs
),

cleaned as (
    select
        series_id,
        observation_year,
        observation_period,
        period_name,

        -- Build a date from year + period (M01-M12 → month 1-12, first of month)
        date_from_parts(
            observation_year,
            try_to_number(substr(observation_period, 2, 2)),
            1
        ) as observation_date,

        -- Cast value to FLOAT, treating '-' sentinel as NULL
        case
            when observation_value_raw = '-' then null
            else try_to_double(observation_value_raw)
        end as observation_value,

        -- Preliminary flag: BLS marks not-yet-final data with footnote code 'P'
        coalesce(footnotes[0]:code::string = 'P', false) as is_preliminary,

        pulled_date

    from flattened
    where observation_period like 'M%'
      and observation_period != 'M13'
)

select * from cleaned