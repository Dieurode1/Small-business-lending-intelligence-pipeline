{{
    config(
        materialized='view'
    )
}}

-- Flattens FRED RAW VARIANT to one row per (series_id, observation_date).
-- Handles FRED quirks: "." sentinel → NULL, date is already an ISO date string.
-- Envelope: raw_response:observations (FRED preserves full envelope;
-- contrast with BLS which is pre-unwrapped).

with source as (
    select
        series_id,
        pulled_date,
        raw_response
    from {{ source('raw', 'fred_series_raw') }}
),

flattened as (
    -- Explode the observations array.
    select
        s.series_id,
        s.pulled_date,
        obs.value:date::date     as observation_date,
        obs.value:value::string  as observation_value_raw
    from source s,
         lateral flatten(input => s.raw_response:observations) obs
),

cleaned as (
    -- Type-cast value, NULL the "." sentinel, derive year for convenience.
    select
        series_id,
        observation_date,
        extract(year from observation_date)::integer  as observation_year,
        case
            when observation_value_raw = '.' then null
            else try_to_double(observation_value_raw)
        end as observation_value,
        pulled_date
    from flattened
)

select * from cleaned
