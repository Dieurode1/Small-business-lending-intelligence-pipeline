{{
    config(
        materialized='view'
    )
}}

-- Census BFS weekly business applications, national total. 1:1 grain with
-- source — natural key is (year, week). All metrics are NSA (Census doesn't
-- publish SA at this granularity per the census.py extractor comment).
-- YoY pct columns are NULL where the prior-year value was zero (was 'inf'
-- in the source pandas extract; converted to NULL via FF_CSV_STANDARD).

with source as (
    select * from {{ source('raw', 'census_bfs_us_apps_weekly') }}
),

cleaned as (
    select
        year                   as observation_year,
        week                   as observation_week,

        -- Application counts
        ba_nsa                 as business_applications,
        hba_nsa                as high_propensity_applications,
        wba_nsa                as wages_applications,
        cba_nsa                as corporate_applications,

        -- Year-over-year percentage changes
        yy_ba_nsa              as business_applications_yoy_pct,
        yy_hba_nsa             as high_propensity_applications_yoy_pct,
        yy_wba_nsa             as wages_applications_yoy_pct,
        yy_cba_nsa             as corporate_applications_yoy_pct,

        _loaded_at             as loaded_at,
        _source_file           as source_file
    from source
)

select * from cleaned
