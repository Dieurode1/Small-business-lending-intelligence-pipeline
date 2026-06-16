{{
    config(
        materialized='view'
    )
}}

-- Census BFS weekly business applications by state (50 states + DC).
-- 1:1 grain with source — natural key is (year, week, state). All
-- metrics are NSA.

with source as (
    select * from {{ source('raw', 'census_bfs_state_apps_weekly') }}
),

cleaned as (
    select
        year                   as observation_year,
        week                   as observation_week,
        state                  as state_code,

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
