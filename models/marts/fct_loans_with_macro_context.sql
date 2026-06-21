{{
    config(
        materialized='table'
    )
}}

-- fct_loans_with_macro_context
-- Monthly loan-origination cohorts joined to the macroeconomic environment
-- at time of approval. Answers: did the macro climate when a loan was
-- approved relate to how that cohort eventually performed?
--
-- Grain: one row per approval_year_month.
--
-- Loan side: all loans approved in a given month, aggregated (count,
-- dollars, outcome counts, true_default_rate) using the same vintage
-- framing as the other marts.
--
-- Macro side: the int_economic_indicators_unified snapshot for that month.
-- Cohorts approved before a given series existed (e.g. pre-2007 for BLS)
-- carry NULLs for that series — expected, not a gap.
--
-- Vintage caveat applies: recent cohorts haven't seasoned, so their
-- default rates are not yet meaningful. Filter to cohorts 5-7+ years old.

with loans as (
    select
        date_trunc('month', approval_date)::date as approval_year_month,
        gross_approval_amount,
        is_paid_in_full,
        is_charged_off,
        is_sba_purchased,
        is_active,
        gross_charge_off_amount
    from {{ ref('int_sba_loans_unified') }}
    where approval_date is not null
),

loan_cohort as (
    select
        approval_year_month,

        count(*)                                                   as total_loans,
        sum(gross_approval_amount)                                 as total_approved_dollars,
        avg(gross_approval_amount)                                 as avg_loan_size,

        sum(case when is_paid_in_full then 1 else 0 end)           as total_paid_in_full,
        sum(case when is_charged_off then 1 else 0 end)            as total_charged_off,
        sum(case when is_sba_purchased then 1 else 0 end)          as total_sba_purchased,
        sum(case when is_active then 1 else 0 end)                 as total_active,
        sum(gross_charge_off_amount)                               as total_charge_off_dollars,

        (sum(case when is_charged_off then 1 else 0 end) * 1.0
            / nullif(count(*), 0))                                 as charge_off_rate,
        ((sum(case when is_charged_off then 1 else 0 end)
          + sum(case when is_sba_purchased then 1 else 0 end)) * 1.0
            / nullif(count(*), 0))                                 as true_default_rate,
        (sum(case when is_paid_in_full then 1 else 0 end) * 1.0
            / nullif(count(*), 0))                                 as paid_in_full_rate,
        (sum(case when is_active then 1 else 0 end) * 1.0
            / nullif(count(*), 0))                                 as active_rate

    from loans
    group by approval_year_month
),

macro as (
    select *
    from {{ ref('int_economic_indicators_unified') }}
)

select
    c.approval_year_month,
    m.year                                  as approval_year,
    m.quarter                               as approval_quarter,

    -- Loan cohort performance
    c.total_loans,
    c.total_approved_dollars,
    c.avg_loan_size,
    c.total_paid_in_full,
    c.total_charged_off,
    c.total_sba_purchased,
    c.total_active,
    c.total_charge_off_dollars,
    c.charge_off_rate,
    c.true_default_rate,
    c.paid_in_full_rate,
    c.active_rate,

    -- Macro environment at approval (FRED)
    m.fred_fed_funds_rate,
    m.fred_treasury_10yr,
    m.fred_unemployment_rate,
    m.fred_cpi,
    m.fred_ci_loans,
    m.fred_recession_probability,
    m.gdp,
    m.gdp_is_carried,
    m.lending_standards,
    m.lending_standards_is_carried,

    -- Macro environment at approval (BLS)
    m.bls_unemployment_rate,
    m.bls_nonfarm_employment,
    m.bls_avg_hourly_earnings,
    m.bls_cpi,
    m.bls_labor_force_participation,
    m.bls_job_openings

from loan_cohort c
left join macro m
    on c.approval_year_month = m.year_month
order by c.approval_year_month
