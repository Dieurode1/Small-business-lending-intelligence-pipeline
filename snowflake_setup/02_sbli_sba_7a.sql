-- ============================================================================
-- 02_SBLI_SBA_7A
-- SBA 7(a) loan-level FOIA data: stage-referenced table + bulk load.
-- Depends on 01_sbli_foundation (SBLI_S3_INTEGRATION, STAGE_SBA_7A,
-- FF_CSV_STANDARD, FF_CSV_PEEK must already exist).
-- ============================================================================
--
-- Source: data.sba.gov 7(a)/504 FOIA — four decade files (1991-1999,
-- 2000-2009, 2010-2019, 2020-present), 43 columns, ~1.93M rows.
-- Lender side is bank-based (FDIC/NCUA). Distinct schema from 504.
--
-- Column types reflect lessons from first load: processingmethod, subprogram,
-- and businessage were widened after VARCHAR(50) truncation errors. State
-- columns stay VARCHAR(2); a handful of source rows have malformed state
-- values (zip codes, 'nan') and are intentionally quarantined via
-- ON_ERROR = CONTINUE rather than corrupting the column contract.


-- ============================================================================
-- BUILD
-- ============================================================================

USE ROLE SBLI_ROLE;
USE WAREHOUSE SBLI_WH;
USE DATABASE SBLI;
USE SCHEMA RAW;

CREATE OR REPLACE TABLE SBA_7A_LOANS (
    -- Loan metadata
    asofdate                       DATE,
    program                        VARCHAR(10),
    l2locid                        VARCHAR(20),

    -- Borrower info
    borrname                       VARCHAR(255),
    borrstreet                     VARCHAR(255),
    borrcity                       VARCHAR(100),
    borrstate                      VARCHAR(2),
    borrzip                        VARCHAR(10),

    -- Bank/lender info
    bankname                       VARCHAR(255),
    bankfdicnumber                 VARCHAR(20),
    bankncuanumber                 VARCHAR(20),
    bankstreet                     VARCHAR(255),
    bankcity                       VARCHAR(100),
    bankstate                      VARCHAR(2),
    bankzip                        VARCHAR(10),

    -- Loan amounts and dates
    grossapproval                  NUMBER(15,2),
    sbaguaranteedapproval          NUMBER(15,2),
    approvaldate                   DATE,
    approvalfiscalyear             NUMBER(4),
    firstdisbursementdate          DATE,

    -- Loan terms
    processingmethod               VARCHAR(255),
    subprogram                     VARCHAR(255),
    initialinterestrate            NUMBER(7,4),
    fixedorvariableinterestind     VARCHAR(10),
    terminmonths                   NUMBER(5),

    -- Industry classification
    naicscode                      VARCHAR(10),
    naicsdescription               VARCHAR(500),
    franchisecode                  VARCHAR(20),
    franchisename                  VARCHAR(255),

    -- Project location and SBA admin
    projectcounty                  VARCHAR(100),
    projectstate                   VARCHAR(2),
    sbadistrictoffice              VARCHAR(100),
    congressionaldistrict          VARCHAR(10),

    -- Business demographics
    businesstype                   VARCHAR(100),
    businessage                    VARCHAR(100),

    -- Loan outcome
    loanstatus                     VARCHAR(50),
    paidinfulldate                 DATE,
    chargeoffdate                  DATE,
    grosschargeoffamount           NUMBER(15,2),
    revolverstatus                 VARCHAR(10),

    -- Jobs and indicators
    jobssupported                  NUMBER(7),
    collateralind                  VARCHAR(10),
    soldsecmrktind                 VARCHAR(10),

    -- Load metadata (added by pipeline, not in source)
    _loaded_at                     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file                   VARCHAR(500)
)
COMMENT = 'Raw SBA 7(a) loan-level FOIA data. 1:1 with source CSVs, no transformations.';

COPY INTO SBA_7A_LOANS (
    asofdate, program, l2locid,
    borrname, borrstreet, borrcity, borrstate, borrzip,
    bankname, bankfdicnumber, bankncuanumber,
    bankstreet, bankcity, bankstate, bankzip,
    grossapproval, sbaguaranteedapproval, approvaldate,
    approvalfiscalyear, firstdisbursementdate,
    processingmethod, subprogram, initialinterestrate,
    fixedorvariableinterestind, terminmonths,
    naicscode, naicsdescription, franchisecode, franchisename,
    projectcounty, projectstate, sbadistrictoffice, congressionaldistrict,
    businesstype, businessage,
    loanstatus, paidinfulldate, chargeoffdate, grosschargeoffamount, revolverstatus,
    jobssupported, collateralind, soldsecmrktind,
    _source_file
)
FROM (
    SELECT
        $1, $2, $3,
        $4, $5, $6, $7, $8,
        $9, $10, $11,
        $12, $13, $14, $15,
        $16, $17, $18,
        $19, $20,
        $21, $22, $23,
        $24, $25,
        $26, $27, $28, $29,
        $30, $31, $32, $33,
        $34, $35,
        $36, $37, $38, $39, $40,
        $41, $42, $43,
        METADATA$FILENAME
    FROM @STAGE_SBA_7A
)
PATTERN = '.*foia_7a_.*\.csv'
ON_ERROR = CONTINUE;


-- ============================================================================
-- VALIDATION  (read-only — confirms the load is correct)
-- ============================================================================

-- Schema discovery (how the table was designed — kept for reproducibility).
-- Run these against a fresh file to re-derive column positions if SBA
-- changes their layout. FF_CSV_PEEK keeps the header row visible.
--   SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
--          $11,$12,$13,$14,$15,$16,$17,$18,$19,$20
--   FROM @STAGE_SBA_7A/<one_file>.csv (FILE_FORMAT => FF_CSV_PEEK) LIMIT 1;
--   ... (repeat for $21-$40, $41-$45 until NULLs confirm 43 columns)

-- Row count — expect ~1,930,936 (≈99.998% of ~1,930,983 parsed;
-- ~47 rows quarantined for malformed state values).
SELECT COUNT(*) AS total_rows FROM SBA_7A_LOANS;

-- All four decade files contributed
SELECT _source_file, COUNT(*) AS rows_loaded
FROM SBA_7A_LOANS
GROUP BY _source_file
ORDER BY _source_file;

-- Sample — confirm columns landed in the right positions
SELECT borrname, borrstate, bankname, grossapproval,
       approvalfiscalyear, naicscode
FROM SBA_7A_LOANS
LIMIT 10;

-- Quarantine spot-check — these patterns are why ON_ERROR = CONTINUE:
-- malformed state values that would violate VARCHAR(2). Expect a small,
-- explainable count, not a systemic failure.
SELECT COUNT(*) AS rows_with_long_state
FROM SBA_7A_LOANS
WHERE LENGTH(borrstate) > 2 OR LENGTH(bankstate) > 2;