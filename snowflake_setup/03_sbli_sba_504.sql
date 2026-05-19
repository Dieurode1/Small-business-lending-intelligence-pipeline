-- ============================================================================
-- 03_SBLI_SBA_504
-- SBA 504 loan-level FOIA data: stage-referenced table + bulk load.
-- Depends on 01_sbli_foundation (SBLI_S3_INTEGRATION, STAGE_SBA_504,
-- FF_CSV_STANDARD, FF_CSV_PEEK must already exist).
-- ============================================================================
--
-- Source: data.sba.gov 7(a)/504 FOIA — two files (1991-2009, 2010-present),
-- 40 columns, ~227K rows.
--
-- 504 is a SEPARATE TABLE from 7(a) by design, not convenience. The programs
-- diverge structurally:
--   - 504 deals are CDC + Third-Party Lender (bank), not a single bank loan.
--     Columns cdc_* and thirdpartylender_* replace 7(a)'s bank* columns.
--   - thirdpartydollars (the bank's portion of the 504 deal) has no 7(a)
--     analog. A 504 deal ≈ 50% TPL + 40% SBA debenture (CDC) + 10% borrower.
--   - No interest-rate / revolver / secondary-market columns: 504 debenture
--     rates are program-set, and 504 is fixed-asset financing, not revolving.
--   - Loan ID column is 'locationid' here vs 'l2locid' in 7(a); fiscal year
--     is 'approvalfy' vs 'approvalfiscalyear'. Same concepts, different names.
--     These get normalized to common names in the dbt staging layer, NOT here.
--
-- Known data pattern: thirdpartylender_name is frequently NULL for early
-- records (TPL counterparty wasn't consistently captured pre-~2015). This is
-- a source reality, not a load defect — handle explicitly in dbt staging.
--
-- Column widths reuse the 7(a) lessons (processingmethod / subprogram /
-- businessage at 255/255/100). State columns stay VARCHAR(2); malformed
-- state values (zip codes, 'nan') are quarantined via ON_ERROR = CONTINUE.


-- ============================================================================
-- BUILD
-- ============================================================================

USE ROLE SBLI_ROLE;
USE WAREHOUSE SBLI_WH;
USE DATABASE SBLI;
USE SCHEMA RAW;

CREATE OR REPLACE TABLE SBA_504_LOANS (
    -- Loan metadata
    asofdate                       DATE,
    program                        VARCHAR(10),
    locationid                     VARCHAR(20),

    -- Borrower info
    borrname                       VARCHAR(255),
    borrstreet                     VARCHAR(255),
    borrcity                       VARCHAR(100),
    borrstate                      VARCHAR(2),
    borrzip                        VARCHAR(10),

    -- CDC (Certified Development Company) info
    cdc_name                       VARCHAR(255),
    cdc_street                     VARCHAR(255),
    cdc_city                       VARCHAR(100),
    cdc_state                      VARCHAR(2),
    cdc_zip                        VARCHAR(10),

    -- Third-party lender (bank) info
    thirdpartylender_name          VARCHAR(255),
    thirdpartylender_city          VARCHAR(100),
    thirdpartylender_state         VARCHAR(2),
    thirdpartydollars              NUMBER(15,2),

    -- Loan amounts and dates
    grossapproval                  NUMBER(15,2),
    approvaldate                   DATE,
    approvalfy                     NUMBER(4),
    firstdisbursementdate          DATE,

    -- Loan terms (no per-loan interest rate — 504 debenture rates are
    -- program-set, so those columns don't exist in this program)
    processingmethod               VARCHAR(255),
    subprogram                     VARCHAR(255),
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

    -- Jobs and indicators
    jobssupported                  NUMBER(7),
    collateralind                  VARCHAR(10),

    -- Load metadata (added by pipeline, not in source)
    _loaded_at                     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file                   VARCHAR(500)
)
COMMENT = 'Raw SBA 504 loan-level FOIA data. 1:1 with source CSVs, no transformations.';

COPY INTO SBA_504_LOANS (
    asofdate, program, locationid,
    borrname, borrstreet, borrcity, borrstate, borrzip,
    cdc_name, cdc_street, cdc_city, cdc_state, cdc_zip,
    thirdpartylender_name, thirdpartylender_city, thirdpartylender_state,
    thirdpartydollars, grossapproval, approvaldate, approvalfy,
    firstdisbursementdate,
    processingmethod, subprogram, terminmonths,
    naicscode, naicsdescription, franchisecode, franchisename,
    projectcounty, projectstate, sbadistrictoffice, congressionaldistrict,
    businesstype, businessage,
    loanstatus, paidinfulldate, chargeoffdate, grosschargeoffamount,
    jobssupported, collateralind,
    _source_file
)
FROM (
    SELECT
        $1, $2, $3,
        $4, $5, $6, $7, $8,
        $9, $10, $11, $12, $13,
        $14, $15, $16,
        $17, $18, $19, $20,
        $21,
        $22, $23, $24,
        $25, $26, $27, $28,
        $29, $30, $31, $32,
        $33, $34,
        $35, $36, $37, $38,
        $39, $40,
        METADATA$FILENAME
    FROM @STAGE_SBA_504
)
PATTERN = '.*foia_504_.*\.csv'
ON_ERROR = CONTINUE;


-- ============================================================================
-- VALIDATION  (read-only — confirms the load is correct)
-- ============================================================================

-- Schema discovery (how the 40-column layout was derived — kept for
-- reproducibility if SBA changes their 504 file format):
--   SELECT $1..$20  FROM @STAGE_SBA_504/<file>.csv
--     (FILE_FORMAT => FF_CSV_PEEK) LIMIT 1;
--   SELECT $21..$40 ... ; SELECT $41..$45 ...  (NULLs confirm 40 columns)

-- Row count — expect ~227,398 (≈99.997% of ~227,404 parsed;
-- ~6 rows quarantined for malformed state values).
SELECT COUNT(*) AS total_rows FROM SBA_504_LOANS;

-- Both files contributed
SELECT _source_file, COUNT(*) AS rows_loaded
FROM SBA_504_LOANS
GROUP BY _source_file
ORDER BY _source_file;

-- Sample — CDC names should look like development companies, TPL names
-- like banks. Note: TPL often NULL for older records (expected).
SELECT borrname, borrstate, cdc_name, thirdpartylender_name,
       grossapproval, approvalfy
FROM SBA_504_LOANS
LIMIT 10;

-- The historical TPL-NULL pattern, made visible: expect a high NULL share
-- in early years, dropping sharply in recent years. Documents the source
-- reality so it isn't mistaken for a load bug.
SELECT
    approvalfy,
    COUNT(*)                                              AS loans,
    COUNT(thirdpartylender_name)                          AS tpl_named,
    ROUND(100 * COUNT(thirdpartylender_name) / COUNT(*), 1) AS tpl_named_pct
FROM SBA_504_LOANS
WHERE approvalfy IS NOT NULL
GROUP BY approvalfy
ORDER BY approvalfy;

-- Quarantine spot-check — malformed state values are why ON_ERROR=CONTINUE.
SELECT COUNT(*) AS rows_with_long_state
FROM SBA_504_LOANS
WHERE LENGTH(borrstate) > 2
   OR LENGTH(cdc_state) > 2
   OR LENGTH(thirdpartylender_state) > 2;