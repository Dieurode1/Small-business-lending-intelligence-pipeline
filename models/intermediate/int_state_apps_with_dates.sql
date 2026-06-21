{{
    config(
        materialized='view'
    )
}}

-- int_state_apps_with_dates
-- Joins state-level Census BFS weekly applications to the date table.
-- Grain: (observation_year, observation_week, state_code) — 51 states × weeks.

with apps as (
    select * from {{ ref('stg_census_bfs_state_apps_weekly') }}
),

dates as (
    select * from {{ ref('stg_census_bfs_date_table') }}
),

joined as (
    select
        apps.observation_year,
        apps.observation_week,
        apps.state_code,

        dates.week_start_date,
        dates.week_end_date,
        extract(month from dates.week_start_date)::integer    as observation_month,
        extract(quarter from dates.week_start_date)::integer  as observation_quarter,
        to_char(dates.week_start_date, 'YYYY-MM')             as observation_year_month,

        apps.business_applications,
        apps.high_propensity_applications,
        apps.wages_applications,
        apps.corporate_applications,

        apps.business_applications_yoy_pct,
        apps.high_propensity_applications_yoy_pct,
        apps.wages_applications_yoy_pct,
        apps.corporate_applications_yoy_pct,

        apps.source_file,
        apps.loaded_at

    from apps
    inner join dates
        on apps.observation_year = dates.observation_year
        and apps.observation_week = dates.observation_week
)

select * from joined
