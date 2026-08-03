{{
    config(
        materialized='view'
    )
}}

-- SBA 504 loan-level data: 1:1 grain with raw (one row per loan record).
-- Light cleaning only — column renames, type casts, drops malformed quarantine.
-- Schema differs from 7(a): CDC + third-party-lender structure replaces the
-- single bank-lender model. No interest rate fields (504 carries different
-- terms via the debenture structure).
--
-- Same surrogate-key pattern as stg_sba_7a_loans: locationid is non-unique
-- (similar to l2locid in 7(a)), so we generate sba_504_loan_row_id via
-- ROW_NUMBER for downstream uniqueness assertions.

with source as (
    select * from {{ source('raw', 'sba_504_loans') }}

    -- Quarantine filter: drop rows where any of the three state columns
    -- has length > 2 (corrupted, e.g. zip codes in state field). Keep
    -- rows with NULL state values (legitimate when not recorded).
    where (borrstate is null or length(borrstate) = 2)
      and (cdc_state is null or length(cdc_state) = 2)
      and (thirdpartylender_state is null or length(thirdpartylender_state) = 2)
),

cleaned as (
    select
        -- Deterministic surrogate row key (guaranteed unique).
        row_number() over (
            order by approvaldate, borrname, grossapproval,
                     cdc_name, naicscode, _source_file
        ) as sba_504_loan_row_id,

        -- Loan identifiers
        locationid                         as loan_id,
        program                            as loan_program,
        asofdate                           as data_asof_date,

        -- Borrower
        borrname                           as borrower_name,
        borrstreet                         as borrower_street,
        borrcity                           as borrower_city,
        borrstate                          as borrower_state,
        borrzip                            as borrower_zip,

        -- CDC (Certified Development Company — nonprofit SBA-backed lender)
        cdc_name                           as cdc_name,
        cdc_street                         as cdc_street,
        cdc_city                           as cdc_city,
        cdc_state                          as cdc_state,
        cdc_zip                            as cdc_zip,

        -- Third-party lender (typically the first-mortgage bank)
        thirdpartylender_name              as third_party_lender_name,
        thirdpartylender_city              as third_party_lender_city,
        thirdpartylender_state             as third_party_lender_state,
        thirdpartydollars                  as third_party_dollars,

        -- Loan amounts and approval
        grossapproval                      as gross_approval_amount,
        approvaldate                       as approval_date,
        approvalfy                         as approval_fiscal_year,

        -- Loan terms
        processingmethod                   as processing_method,
        terminmonths                       as term_in_months,

        -- Industry classification
        naicscode                          as naics_code,
        naicsdescription                   as naics_description,

        -- Project location
        projectcounty                      as project_county,
        projectstate                       as project_state,
        sbadistrictoffice                  as sba_district_office,

        -- Business demographics
        businesstype                       as business_type,
        businessage                        as business_age,

        -- Loan outcome
        loanstatus                         as loan_status,
        paidinfulldate                     as paid_in_full_date,
        chargeoffdate                      as charge_off_date,
        grosschargeoffamount               as gross_charge_off_amount,

        -- Jobs
        jobssupported                      as jobs_supported,

        -- Load metadata
        _loaded_at                         as loaded_at,
        _source_file                       as source_file

    from source
)

select * from cleaned
