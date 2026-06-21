{{
    config(
        materialized='table'
    )
}}

-- fct_loan_performance_by_industry
-- Industry-level performance metrics aggregated by approval fiscal year.
-- Grain: one row per (naics_code, approval_fiscal_year).
--
-- Built at the natural 6-digit NAICS grain. naics_sector_code and
-- naics_sector_name (first 2 digits, mapped to sector name) are surfaced
-- as dimension columns so consumers can roll up to sector level at query
-- time without losing sub-sector detail. Tiny-bucket statistical noise is
-- handled at query time with a total_loans > N filter, same as
-- fct_lender_performance.
--
-- Vintage analysis framing: each row is loans ORIGINATED in FY[X] for a
-- given industry, with metrics reflecting eventual performance.
--
-- Headline metric: true_default_rate = (charged_off + sba_purchased) / total.

with loans as (
    select *
    from {{ ref('int_sba_loans_unified') }}
),

aggregated as (
    select
        coalesce(naics_code, 'UNKNOWN')                            as naics_code,
        max(naics_description)                                     as naics_description,
        approval_fiscal_year,

        -- Volume counts
        count(*)                                                   as total_loans,
        sum(gross_approval_amount)                                 as total_approved_dollars,
        avg(gross_approval_amount)                                 as avg_loan_size,
        count(distinct primary_lender_name)                        as distinct_lenders,
        count(distinct project_state)                              as distinct_states,

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
    group by 1, 3
),

final as (
    select
        naics_code,

        -- Derived NAICS sector (first 2 digits, with collapsed ranges)
        case
            when naics_code = 'UNKNOWN' then 'UNKNOWN'
            else left(naics_code, 2)
        end                                                        as naics_sector_code,

        case
            when naics_code = 'UNKNOWN'                            then 'Unknown'
            when left(naics_code, 2) = '11'                        then 'Agriculture, Forestry, Fishing & Hunting'
            when left(naics_code, 2) = '21'                        then 'Mining, Quarrying & Oil/Gas Extraction'
            when left(naics_code, 2) = '22'                        then 'Utilities'
            when left(naics_code, 2) = '23'                        then 'Construction'
            when left(naics_code, 2) in ('31', '32', '33')        then 'Manufacturing'
            when left(naics_code, 2) = '42'                        then 'Wholesale Trade'
            when left(naics_code, 2) in ('44', '45')              then 'Retail Trade'
            when left(naics_code, 2) in ('48', '49')              then 'Transportation & Warehousing'
            when left(naics_code, 2) = '51'                        then 'Information'
            when left(naics_code, 2) = '52'                        then 'Finance & Insurance'
            when left(naics_code, 2) = '53'                        then 'Real Estate & Rental/Leasing'
            when left(naics_code, 2) = '54'                        then 'Professional, Scientific & Technical Services'
            when left(naics_code, 2) = '55'                        then 'Management of Companies & Enterprises'
            when left(naics_code, 2) = '56'                        then 'Administrative & Waste Services'
            when left(naics_code, 2) = '61'                        then 'Educational Services'
            when left(naics_code, 2) = '62'                        then 'Health Care & Social Assistance'
            when left(naics_code, 2) = '71'                        then 'Arts, Entertainment & Recreation'
            when left(naics_code, 2) = '72'                        then 'Accommodation & Food Services'
            when left(naics_code, 2) = '81'                        then 'Other Services (except Public Admin)'
            when left(naics_code, 2) = '92'                        then 'Public Administration'
            else 'Other / Unclassified'
        end                                                        as naics_sector_name,

        naics_description,
        approval_fiscal_year,

        -- Volume
        total_loans,
        total_approved_dollars,
        avg_loan_size,
        distinct_lenders,
        distinct_states,

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
