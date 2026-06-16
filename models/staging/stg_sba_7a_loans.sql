{{
    config(
        materialized='view'
    )
}}

-- SBA 7(a) loan-level data: 1:1 grain with raw (one row per loan record).
-- Light cleaning only — column renames, type casts, drops malformed quarantine.
-- Schema differs from 504 (bank-based lender vs. CDC + third-party-lender).
--
-- IMPORTANT — natural key: SBA FOIA doesn't have a unique loan identifier.
-- Real-world franchise/multi-location borrowers can produce multiple identical
-- (borrower, date, amount) rows (e.g., a franchise operator opening 20
-- locations gets 20 distinct loans booked the same day). l2locid is a
-- non-unique reference (~6,800 distinct values across 1.9M rows). We add a
-- deterministic surrogate row key (sba_7a_loan_row_id) for downstream
-- uniqueness enforcement and joins.

with source as (
    select * from {{ source('raw', 'sba_7a_loans') }}

    -- Drop the quarantine survivors: rows where SBA's FOIA data has zip
    -- codes (length > 2) in the state columns. Keep rows where state is
    -- NULL (legitimate — bank state isn't always recorded).
    where (borrstate is null or length(borrstate) = 2)
      and (bankstate is null or length(bankstate) = 2)
),

cleaned as (
    select
        -- Deterministic surrogate row key (guaranteed unique).
        -- Uses source-side column names since the ROW_NUMBER runs against
        -- the source CTE before column renames take effect.
        row_number() over (
            order by approvaldate, borrname, grossapproval,
                     bankname, naicscode, _source_file
        ) as sba_7a_loan_row_id,

        -- Loan identifiers
        l2locid                            as loan_id,
        program                            as loan_program,
        asofdate                           as data_asof_date,

        -- Borrower
        borrname                           as borrower_name,
        borrstreet                         as borrower_street,
        borrcity                           as borrower_city,
        borrstate                          as borrower_state,
        borrzip                            as borrower_zip,

        -- Lender (bank-based)
        bankname                           as bank_name,
        bankfdicnumber                     as bank_fdic_number,
        bankncuanumber                     as bank_ncua_number,
        bankstreet                         as bank_street,
        bankcity                           as bank_city,
        bankstate                          as bank_state,
        bankzip                            as bank_zip,

        -- Loan amounts and approval
        grossapproval                      as gross_approval_amount,
        sbaguaranteedapproval              as sba_guaranteed_amount,
        approvaldate                       as approval_date,
        approvalfiscalyear                 as approval_fiscal_year,

        -- Loan terms
        processingmethod                   as processing_method,
        subprogram                         as sub_program,
        initialinterestrate                as initial_interest_rate,
        fixedorvariableinterestind         as interest_rate_type,
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
