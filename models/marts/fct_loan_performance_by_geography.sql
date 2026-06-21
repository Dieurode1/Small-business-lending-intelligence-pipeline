{{
    config(
        materialized='table'
    )
}}

-- fct_loan_performance_by_geography
-- State-level performance metrics aggregated by approval fiscal year.
-- Grain: one row per (project_state, approval_fiscal_year).
--
-- Uses project_state (where the funded economic activity lands) rather
-- than borrower_state. census_region is derived so this mart can later
-- join cleanly to Census BFS business-formation data by region.
--
-- Vintage analysis framing: each row is loans ORIGINATED in FY[X] in a
-- given state, with metrics reflecting eventual performance.
--
-- Headline metric: true_default_rate = (charged_off + sba_purchased) / total.

with loans as (
    select *
    from {{ ref('int_sba_loans_unified') }}
    where project_state is not null
),

aggregated as (
    select
        project_state,
        approval_fiscal_year,

        -- Volume counts
        count(*)                                                   as total_loans,
        sum(gross_approval_amount)                                 as total_approved_dollars,
        avg(gross_approval_amount)                                 as avg_loan_size,
        count(distinct primary_lender_name)                        as distinct_lenders,
        count(distinct naics_code)                                 as distinct_naics_served,

        -- Outcome counts
        sum(case when is_paid_in_full then 1 else 0 end)           as total_paid_in_full,
        sum(case when is_charged_off then 1 else 0 end)            as total_charged_off,
        sum(case when is_sba_purchased then 1 else 0 end)          as total_sba_purchased,
        sum(case when is_active then 1 else 0 end)                 as total_active,
        sum(gross_charge_off_amount)                               as total_charge_off_dollars,

        -- Program breakdown
        sum(case when loan_program = '7A' then 1 else 0 end)       as loans_7a,
        sum(case when loan_program = '504' then 1 else 0 end)      as loans_504

    from loans
    group by 1, 2
),

final as (
    select
        project_state,

        -- Derived Census region (4-region scheme)
        case
            when project_state in ('CT','ME','MA','NH','RI','VT','NJ','NY','PA')
                then 'Northeast'
            when project_state in ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD')
                then 'Midwest'
            when project_state in ('DE','DC','FL','GA','MD','NC','SC','VA','WV','AL','KY','MS','TN','AR','LA','OK','TX')
                then 'South'
            when project_state in ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA')
                then 'West'
            else 'US Territories'
        end                                                        as census_region,

        approval_fiscal_year,

        -- Volume
        total_loans,
        total_approved_dollars,
        avg_loan_size,
        distinct_lenders,
        distinct_naics_served,

        -- Outcome counts
        total_paid_in_full,
        total_charged_off,
        total_sba_purchased,
        total_active,
        total_charge_off_dollars,

        -- Headline rates
        (total_charged_off * 1.0 / nullif(total_loans, 0))                              as charge_off_rate,
        ((total_charged_off + total_sba_purchased) * 1.0 / nullif(total_loans, 0))      as true_default_rate,
        (total_paid_in_full * 1.0 / nullif(total_loans, 0))                             as paid_in_full_rate,
        (total_active * 1.0 / nullif(total_loans, 0))                                   as active_rate,

        -- Program mix
        case
            when loans_7a > loans_504 then '7A'
            when loans_504 > loans_7a then '504'
            else 'MIXED'
        end                                                        as primary_program,
        loans_7a,
        loans_504

    from aggregated
)

select * from final
