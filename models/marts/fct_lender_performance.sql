{{
    config(
        materialized='table'
    )
}}

-- fct_lender_performance
-- Lender-level performance metrics aggregated by approval fiscal year.
-- Grain: one row per (lender_name, lender_type, approval_fiscal_year).
--
-- Vintage analysis framing: each row represents "loans this lender
-- originated in FY[X]" with performance metrics reflecting how those
-- originations eventually performed across their full lifecycle.
-- This is the canonical credit-risk view — answers "what's the vintage
-- performance of this lender's originations?" not "what's the current
-- portfolio state?"
--
-- Headline metric: true_default_rate = (charged_off + sba_purchased) / total.
-- This is higher than the formal charge-off rate but reflects actual
-- credit losses including SBA's guarantee payouts.
--
-- Materialized as a table because this is consumer-facing — analysts and
-- BI tools will query it constantly, so pre-computing once is the right
-- tradeoff vs. recomputing aggregations on every query.

with loans as (
    select *
    from {{ ref('int_sba_loans_unified') }}
    where primary_lender_name is not null
),

-- One row per (lender, year) with summed metrics
aggregated as (
    select
        primary_lender_name                                        as lender_name,
        lender_type,
        approval_fiscal_year,

        -- Volume counts
        count(*)                                                   as total_loans,
        sum(gross_approval_amount)                                 as total_approved_dollars,
        count(distinct project_state)                              as distinct_states_served,
        count(distinct naics_code)                                 as distinct_naics_served,

        -- Outcome counts
        sum(case when is_paid_in_full then 1 else 0 end)           as total_paid_in_full,
        sum(case when is_charged_off then 1 else 0 end)            as total_charged_off,
        sum(case when is_sba_purchased then 1 else 0 end)          as total_sba_purchased,
        sum(case when is_active then 1 else 0 end)                 as total_active,

        -- Dollar metrics
        avg(gross_approval_amount)                                 as avg_loan_size,
        sum(gross_charge_off_amount)                               as total_charge_off_dollars,

        -- Program breakdown
        sum(case when loan_program = '7A' then 1 else 0 end)       as loans_7a,
        sum(case when loan_program = '504' then 1 else 0 end)      as loans_504

    from loans
    group by 1, 2, 3
),

-- Identify each lender's primary state (by dollar volume) per year
primary_state_per_lender_year as (
    select
        primary_lender_name,
        lender_type,
        approval_fiscal_year,
        project_state,
        sum(gross_approval_amount) as state_dollars,
        row_number() over (
            partition by primary_lender_name, lender_type, approval_fiscal_year
            order by sum(gross_approval_amount) desc
        ) as state_rank
    from loans
    where project_state is not null
    group by 1, 2, 3, 4
),

-- Combine: add primary_state and compute rates
final as (
    select
        a.lender_name,
        a.lender_type,
        a.approval_fiscal_year,

        -- Volume
        a.total_loans,
        a.total_approved_dollars,
        a.avg_loan_size,

        -- Outcome counts
        a.total_paid_in_full,
        a.total_charged_off,
        a.total_sba_purchased,
        a.total_active,
        a.total_charge_off_dollars,

        -- Headline rates (multiply by 1.0 to force float division)
        (a.total_charged_off * 1.0 / nullif(a.total_loans, 0))                                    as charge_off_rate,
        ((a.total_charged_off + a.total_sba_purchased) * 1.0 / nullif(a.total_loans, 0))          as true_default_rate,
        (a.total_paid_in_full * 1.0 / nullif(a.total_loans, 0))                                   as paid_in_full_rate,
        (a.total_active * 1.0 / nullif(a.total_loans, 0))                                         as active_rate,

        -- Geographic and industry diversification
        a.distinct_states_served,
        a.distinct_naics_served,
        ps.project_state                                                                          as primary_state,

        -- Program mix
        case
            when a.loans_7a > a.loans_504 then '7A'
            when a.loans_504 > a.loans_7a then '504'
            else 'MIXED'
        end                                                                                       as primary_program,
        a.loans_7a,
        a.loans_504

    from aggregated a
    left join primary_state_per_lender_year ps
        on a.lender_name = ps.primary_lender_name
        and a.lender_type = ps.lender_type
        and a.approval_fiscal_year = ps.approval_fiscal_year
        and ps.state_rank = 1
)

select * from final
