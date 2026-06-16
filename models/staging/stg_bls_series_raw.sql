{{
    config(
        materialized='view'
    )
}}

-- Flattens BLS RAW VARIANT to one row per (series_id, year, period).
-- Monthly grain; M13 annual averages filtered out.
-- Handles BLS quirks: "-" sentinel → NULL, preliminary flag from footnotes.
-- Envelope: raw_response:data (bls.py pre-unwraps; FRED preserves envelope).

with source as (
    select
        series_id,
        pulled_date,
        raw_response
    from {{ source('raw', 'bls_series_raw') }}
),

flattened as (
    -- Explode the observations array.
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
    -- Type-cast, NULL the sentinel, surface preliminary flag, filter to monthly.
    select
        series_id,
        observation_year,
        observation_period,
        period_name,

        date_from_parts(
            observation_year,
            try_to_number(substr(observation_period, 2, 2)),
            1
        ) as observation_date,

        case
            when observation_value_raw = '-' then null
            else try_to_double(observation_value_raw)
        end as observation_value,

        coalesce(footnotes[0]:code::string = 'P', false) as is_preliminary,

        pulled_date

    from flattened
    where observation_period like 'M%'
      and observation_period != 'M13'
)

select * from cleaned
