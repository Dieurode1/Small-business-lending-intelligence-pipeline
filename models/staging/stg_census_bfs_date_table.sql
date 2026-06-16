{{
    config(
        materialized='view'
    )
}}

-- Census BFS week-to-date lookup. Joined to the three apps_weekly fact
-- tables in intermediate models to attach human-readable week start/end
-- dates. 1:1 grain with source — natural key is (year, week).

with source as (
    select * from {{ source('raw', 'census_bfs_date_table') }}
),

cleaned as (
    select
        year                   as observation_year,
        week                   as observation_week,
        start_date             as week_start_date,
        end_date               as week_end_date,
        _loaded_at             as loaded_at,
        _source_file           as source_file
    from source
)

select * from cleaned
