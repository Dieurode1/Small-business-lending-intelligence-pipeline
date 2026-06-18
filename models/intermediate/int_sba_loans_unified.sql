{{
    config(
        materialized='view'
    )
}}

-- int_sba_loans_unified
-- Unions stg_sba_7a_loans and stg_sba_504_loans into a common loan
-- population. Grain: one row per loan. Total ~2.17M rows
-- (1.95M from 7a + 227K from 504).
--
-- Design decisions documented in design notes:
--   - Generic + program-specific lender columns side-by-side (NULLs carry
--     information — cdc_name IS NULL means it's a 7(a) loan)
--   - Status normalized to canonical taxonomy with both granular and
--     coarse rollup columns
--   - is_sba_purchased flagged separately from is_charged_off because
--     "true default rate" in credit risk = charge-offs + SBA purchases

with sba_7a as (
    select
        -- Surrogate key prefixed for cross-program uniqueness
        '7A_' || sba_7a_loan_row_id::string  as sba_loan_row_id,
        loan_program,

        -- Borrower
        borrower_name,
        borrower_state,
        borrower_zip,
        borrower_city,

        -- Generic lender (7a uses the bank as the SBA-facing lender)
        bank_name                            as primary_lender_name,
        bank_state                           as primary_lender_state,
        'BANK'                               as lender_type,

        -- Program-specific lender columns (7a populated, 504 nulls)
        bank_name,
        bank_fdic_number,
        cast(null as varchar)                as cdc_name,
        cast(null as varchar)                as third_party_lender_name,
        cast(null as number(15,2))           as third_party_dollars,

        -- Amounts and approval
        gross_approval_amount,
        sba_guaranteed_amount,
        approval_date,
        approval_fiscal_year,

        -- Industry and location
        naics_code,
        naics_description,
        project_state,
        project_county,

        -- Outcome
        loan_status                          as loan_status_raw,
        paid_in_full_date,
        charge_off_date,
        gross_charge_off_amount,
        jobs_supported,

        -- Lineage
        source_file,
        loaded_at

    from {{ ref('stg_sba_7a_loans') }}
),

sba_504 as (
    select
        '504_' || sba_504_loan_row_id::string  as sba_loan_row_id,
        loan_program,

        borrower_name,
        borrower_state,
        borrower_zip,
        borrower_city,

        -- 504 uses the CDC as the SBA-facing lender
        cdc_name                             as primary_lender_name,
        cdc_state                            as primary_lender_state,
        'CDC'                                as lender_type,

        -- 504-only nulls for bank columns, populated CDC + third-party
        cast(null as varchar)                as bank_name,
        cast(null as varchar)                as bank_fdic_number,
        cdc_name,
        third_party_lender_name,
        third_party_dollars,

        gross_approval_amount,
        cast(null as number(15,2))           as sba_guaranteed_amount,  -- 504 has no guarantee field
        approval_date,
        approval_fiscal_year,

        naics_code,
        naics_description,
        project_state,
        project_county,

        loan_status                          as loan_status_raw,
        paid_in_full_date,
        charge_off_date,
        gross_charge_off_amount,
        jobs_supported,

        source_file,
        loaded_at

    from {{ ref('stg_sba_504_loans') }}
),

unioned as (
    select * from sba_7a
    union all
    select * from sba_504
),

normalized as (
    select
        *,

        -- Canonical status normalization across both programs
        case
            -- PAID_IN_FULL variants
            when loan_status_raw in ('P I F', 'PAID IN FULL', 'PREPAID IN FULL', 'PAID IN FULL (LIQ)')
                then 'PAID_IN_FULL'

            -- CHARGED_OFF variants (CLSLN is a charge-off in disguise — 99.96% have charge_off_date)
            when loan_status_raw in ('CHGOFF', 'CLSLN', 'CHARGED-OFF')
                then 'CHARGED_OFF'

            -- CURRENT
            when loan_status_raw in ('CURR', 'CURRENT')
                then 'CURRENT'

            -- PAST_DUE variants (504 breaks out months past due; we collapse)
            when loan_status_raw in ('PSTDUE', 'DELINQ')
                or loan_status_raw like '%PAST DUE%'
                then 'PAST_DUE'

            when loan_status_raw in ('DEFERD', 'IN DEFERMENT')
                then 'IN_DEFERMENT'

            when loan_status_raw in ('CANCLD', 'CANCELED')
                then 'CANCELED'

            when loan_status_raw = 'NOT FUNDED'
                then 'NOT_FUNDED'

            when loan_status_raw = 'LIQUID'
                then 'IN_LIQUIDATION'

            when loan_status_raw = 'IN CATCH-UP'
                then 'IN_CATCH_UP'

            when loan_status_raw = 'PURCH(NOT C/O)'
                then 'SBA_PURCHASED'

            when loan_status_raw = 'COMMIT'
                then 'COMMITTED'

            when loan_status_raw in ('SOLDNC', 'SOLDCO', 'ASSET SALE')
                then 'SOLD'

            when loan_status_raw = 'PURCHASE PENDING'
                then 'PURCHASE_PENDING'

            else 'UNKNOWN'
        end as loan_status,

        -- Months-past-due detail extracted from 504-style strings
        -- (504 has '1 MONTH PAST DUE' through '>9 MONTHS PAST DUE')
        case
            when loan_status_raw like '%MONTH%PAST DUE%'
                then try_to_number(regexp_substr(loan_status_raw, '\\d+'))
            else null
        end as months_past_due

    from unioned
),

with_derived as (
    select
        *,

        -- Coarse category for high-level analytics
        case
            when loan_status = 'PAID_IN_FULL'                                           then 'COMPLETED_PAID'
            when loan_status = 'CHARGED_OFF'                                            then 'COMPLETED_LOSS'
            when loan_status in ('CURRENT', 'PAST_DUE', 'IN_DEFERMENT', 'IN_CATCH_UP',
                                 'IN_LIQUIDATION')                                      then 'ACTIVE'
            when loan_status in ('CANCELED', 'NOT_FUNDED', 'COMMITTED', 'PURCHASE_PENDING')
                then 'INACTIVE'
            when loan_status in ('SBA_PURCHASED', 'SOLD')                               then 'RESOLVED_OTHER'
            else 'UNKNOWN'
        end as loan_status_category,

        -- Boolean flags for the common analytical questions
        (loan_status = 'PAID_IN_FULL')                                                  as is_paid_in_full,
        (loan_status = 'CHARGED_OFF')                                                   as is_charged_off,
        (loan_status = 'SBA_PURCHASED')                                                 as is_sba_purchased,
        (loan_status in ('CURRENT', 'PAST_DUE', 'IN_DEFERMENT', 'IN_CATCH_UP',
                         'IN_LIQUIDATION'))                                             as is_active,

        -- Approval period grains for joins to economic indicators
        extract(year from approval_date)::integer    as approval_year,
        extract(month from approval_date)::integer   as approval_month,
        extract(quarter from approval_date)::integer as approval_quarter

    from normalized
)

select * from with_derived
