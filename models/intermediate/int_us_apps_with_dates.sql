{{
    config(
        materialized='view'
    )
}}

-- int_us_apps_with_dates
-- Joins national-level Census BFS weekly applications to the date table.
-- Grain: (observation_year, observation_week) — one row per week.
-- INNER JOIN with row-count test downstream asserts no apps rows are dropped.

with apps as (
    select * from {{ ref('stg_census_bfs_us_apps_weekly') }}
),

dates as (
    select * from {{ ref('stg_census_bfs_date_table') }}
),

joined as (
    select
        -- Grain columns
        apps.observation_year,
        apps.observation_week,

        -- Date context from lookup
        dates.week_start_date,
        dates.week_end_date,

        -- Derived period columns for downstream rollups
        extract(month from dates.week_start_date)::integer    as observation_month,
        extract(quarter from dates.week_start_date)::integer  as observation_quarter,
        to_char(dates.week_start_date, 'YYYY-MM')             as observation_year_month,

        -- Application metrics (NSA across the board)
        apps.business_applications,
        apps.high_propensity_applications,
        apps.wages_applications,
        apps.corporate_applications,

        -- Year-over-year percent changes
        apps.business_applications_yoy_pct,
        apps.high_propensity_applications_yoy_pct,
        apps.wages_applications_yoy_pct,
        apps.corporate_applications_yoy_pct,

        -- Lineage
        apps.source_file,
        apps.loaded_at

    from apps
    inner join dates
        on apps.observation_year = dates.observation_year
        and apps.observation_week = dates.observation_week
)

select * from joined
