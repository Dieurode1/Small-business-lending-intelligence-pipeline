{{
    config(
        materialized='view'
    )
}}

-- int_economic_indicators_unified
-- A monthly "macro spine": one row per calendar month, with each economic
-- indicator resampled to a monthly grain. This is the reusable economic
-- context layer that fct_loans_with_macro_context joins onto.
--
-- Grain: one row per year_month (first-of-month date).
--
-- Resampling rules:
--   - Monthly series (most of them): used as-is.
--   - Daily series (FRED DGS10, 10-yr Treasury): monthly AVERAGE.
--   - Quarterly series (FRED GDP, DRTSCILM): forward-filled across the two
--     non-publishing months of each quarter, with *_is_carried flags.
--
-- Cross-validation note: UNRATE (FRED) and LNS14000000 (BLS) are the same
-- unemployment series; CPIAUCSL (FRED) and CUUR0000SA0 (BLS) are both CPI.
-- Both surfaced with fred_/bls_ prefixes so the cross-source check is explicit.
--
-- Series have different start dates (GDP 1946, DGS10 1962, DRTSCILM 1990,
-- BLS 2007). The spine spans the full range; a series is NULL before it
-- existed. Expected, not a gap to fill.

with fred as (
    select
        date_trunc('month', observation_date)::date as year_month,
        series_id,
        observation_value,
        observation_date
    from {{ ref('stg_fred_series_raw') }}
),

bls as (
    select
        date_trunc('month', observation_date)::date as year_month,
        series_id,
        observation_value
    from {{ ref('stg_bls_series_raw') }}
),

date_bounds as (
    select
        min(year_month) as min_month,
        max(year_month) as max_month
    from (
        select year_month from fred
        union all
        select year_month from bls
    )
),

month_spine as (
    select year_month from (
        select
            dateadd('month', seq4(), (select min_month from date_bounds))::date as year_month
        from table(generator(rowcount => 1200))
    )
    where year_month <= (select max_month from date_bounds)
),

fred_monthly as (
    select
        year_month,
        max(case when series_id = 'FEDFUNDS'      then observation_value end) as fred_fed_funds_rate,
        max(case when series_id = 'UNRATE'        then observation_value end) as fred_unemployment_rate,
        max(case when series_id = 'CPIAUCSL'      then observation_value end) as fred_cpi,
        max(case when series_id = 'BUSLOANS'      then observation_value end) as fred_ci_loans,
        max(case when series_id = 'RECPROUSM156N' then observation_value end) as fred_recession_probability
    from fred
    where series_id in ('FEDFUNDS','UNRATE','CPIAUCSL','BUSLOANS','RECPROUSM156N')
    group by year_month
),

fred_daily_resampled as (
    select
        year_month,
        avg(observation_value) as fred_treasury_10yr
    from fred
    where series_id = 'DGS10'
      and observation_value is not null
    group by year_month
),

fred_quarterly as (
    select
        year_month,
        max(case when series_id = 'GDP'      then observation_value end) as gdp_published,
        max(case when series_id = 'DRTSCILM' then observation_value end) as lending_standards_published
    from fred
    where series_id in ('GDP','DRTSCILM')
    group by year_month
),

bls_monthly as (
    select
        year_month,
        max(case when series_id = 'LNS14000000'            then observation_value end) as bls_unemployment_rate,
        max(case when series_id = 'CES0000000001'          then observation_value end) as bls_nonfarm_employment,
        max(case when series_id = 'CES0500000003'          then observation_value end) as bls_avg_hourly_earnings,
        max(case when series_id = 'CUUR0000SA0'            then observation_value end) as bls_cpi,
        max(case when series_id = 'LNS11300000'            then observation_value end) as bls_labor_force_participation,
        max(case when series_id = 'JTS000000000000000JOL'  then observation_value end) as bls_job_openings
    from bls
    group by year_month
),

joined as (
    select
        s.year_month,
        extract(year from s.year_month)::integer    as year,
        extract(month from s.year_month)::integer   as month,
        extract(quarter from s.year_month)::integer as quarter,

        fm.fred_fed_funds_rate,
        fm.fred_unemployment_rate,
        fm.fred_cpi,
        fm.fred_ci_loans,
        fm.fred_recession_probability,

        fd.fred_treasury_10yr,

        fq.gdp_published,
        fq.lending_standards_published,

        bm.bls_unemployment_rate,
        bm.bls_nonfarm_employment,
        bm.bls_avg_hourly_earnings,
        bm.bls_cpi,
        bm.bls_labor_force_participation,
        bm.bls_job_openings

    from month_spine s
    left join fred_monthly          fm on s.year_month = fm.year_month
    left join fred_daily_resampled  fd on s.year_month = fd.year_month
    left join fred_quarterly        fq on s.year_month = fq.year_month
    left join bls_monthly           bm on s.year_month = bm.year_month
),

filled as (
    select
        *,

        last_value(gdp_published ignore nulls) over (
            order by year_month
            rows between unbounded preceding and current row
        ) as gdp,
        (gdp_published is null
            and last_value(gdp_published ignore nulls) over (
                order by year_month rows between unbounded preceding and current row
            ) is not null) as gdp_is_carried,

        last_value(lending_standards_published ignore nulls) over (
            order by year_month
            rows between unbounded preceding and current row
        ) as lending_standards,
        (lending_standards_published is null
            and last_value(lending_standards_published ignore nulls) over (
                order by year_month rows between unbounded preceding and current row
            ) is not null) as lending_standards_is_carried

    from joined
)

select
    year_month,
    year,
    month,
    quarter,

    fred_fed_funds_rate,
    fred_treasury_10yr,
    fred_unemployment_rate,
    fred_cpi,
    fred_ci_loans,
    fred_recession_probability,
    gdp,
    gdp_is_carried,
    lending_standards,
    lending_standards_is_carried,

    bls_unemployment_rate,
    bls_nonfarm_employment,
    bls_avg_hourly_earnings,
    bls_cpi,
    bls_labor_force_participation,
    bls_job_openings

from filled
order by year_month
