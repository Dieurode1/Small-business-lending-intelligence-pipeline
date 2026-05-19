-- ============================================================================
-- 01_SBLI_FOUNDATION
-- Shared infrastructure for the SBLI (Small Business Lending Intelligence)
-- pipeline. Run this first — every source worksheet depends on objects
-- created here.
-- ============================================================================
--
-- ┌────────────────────────────────────────────────────────────────────────┐
-- │ ⚠  PLACEHOLDERS — READ BEFORE RUNNING                                    │
-- │                                                                          │
-- │ This file contains two environment-specific placeholders that MUST be    │
-- │ replaced before running on a fresh environment:                          │
-- │                                                                          │
-- │   <YOUR_SNOWFLAKE_USERNAME>  — your Snowflake login username             │
-- │   <YOUR_AWS_ROLE_ARN>        — the IAM role ARN for the S3 integration   │
-- │                                 (arn:aws:iam::<acct>:role/...)           │
-- │                                                                          │
-- │ They are intentionally left as placeholders so this file can be          │
-- │ committed to a public repo WITHOUT leaking the AWS account ID or         │
-- │ Snowflake username. Fill them in locally at run time; never commit       │
-- │ the real values.                                                         │
-- └────────────────────────────────────────────────────────────────────────┘


-- ============================================================================
-- BUILD
-- ============================================================================

-- ---- Warehouse, database, schemas, role ----------------------------------
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS SBLI_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for SBLI ingestion, dbt builds, and ad-hoc queries';

CREATE DATABASE IF NOT EXISTS SBLI
    COMMENT = 'Small Business Lending Intelligence platform';

USE DATABASE SBLI;

CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Landing zone: COPY INTO targets, 1:1 with S3 source files';
CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'dbt staging models: cleaned, typed, renamed from raw';
CREATE SCHEMA IF NOT EXISTS INTERMEDIATE
    COMMENT = 'dbt intermediate models: joins, dedup, business logic';
CREATE SCHEMA IF NOT EXISTS MARTS
    COMMENT = 'dbt mart models: analytics-ready, consumed by Streamlit';

CREATE ROLE IF NOT EXISTS SBLI_ROLE
    COMMENT = 'Owns SBLI database operations: ingestion + transformation';

GRANT USAGE, OPERATE ON WAREHOUSE SBLI_WH TO ROLE SBLI_ROLE;
GRANT USAGE ON DATABASE SBLI TO ROLE SBLI_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE SBLI TO ROLE SBLI_ROLE;
GRANT CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT
    ON ALL SCHEMAS IN DATABASE SBLI TO ROLE SBLI_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
    ON FUTURE TABLES IN DATABASE SBLI TO ROLE SBLI_ROLE;

-- ⚠ PLACEHOLDER: replace <YOUR_SNOWFLAKE_USERNAME> with your Snowflake username
GRANT ROLE SBLI_ROLE TO USER <YOUR_SNOWFLAKE_USERNAME>;


-- ---- Storage integration --------------------------------------------------
-- ⚠ PLACEHOLDER: replace <YOUR_AWS_ROLE_ARN> with the IAM role ARN
--   (arn:aws:iam::<acct>:role/snowflake-sbli-s3-access).
-- After creating, run DESC INTEGRATION (validation section) and update the
-- IAM trust policy with STORAGE_AWS_IAM_USER_ARN + STORAGE_AWS_EXTERNAL_ID.
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION SBLI_S3_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = '<YOUR_AWS_ROLE_ARN>'
    STORAGE_ALLOWED_LOCATIONS = ('s3://sbli-platform-raw-dt/')
    COMMENT = 'Read-only access to SBLI raw bucket';

GRANT USAGE ON INTEGRATION SBLI_S3_INTEGRATION TO ROLE SBLI_ROLE;


-- ---- File formats ---------------------------------------------------------
USE ROLE SBLI_ROLE;
USE DATABASE SBLI;
USE SCHEMA RAW;

-- Standard CSV: handles SBA 7(a), SBA 504, Census BFS.
-- NULL_IF includes pandas artifacts (nan / inf) discovered during loading.
CREATE OR REPLACE FILE FORMAT FF_CSV_STANDARD
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL', 'null', 'N/A', 'n/a', 'nan', 'NaN', 'NAN',
               'inf', '-inf', 'Inf', '-Inf', 'INF', '-INF')
    EMPTY_FIELD_AS_NULL = TRUE
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    REPLACE_INVALID_CHARACTERS = TRUE
    DATE_FORMAT = 'AUTO'
    TIMESTAMP_FORMAT = 'AUTO'
    COMMENT = 'Standard CSV: comma-delimited, quoted strings, header row, lenient nulls (incl. pandas inf/nan)';

-- Standard JSON: handles FRED and BLS (full document into VARIANT).
CREATE OR REPLACE FILE FORMAT FF_JSON_STANDARD
    TYPE = JSON
    STRIP_OUTER_ARRAY = FALSE
    COMPRESSION = AUTO
    COMMENT = 'Standard JSON: full API responses preserved as VARIANT';

-- Peek format: header row NOT skipped, used only to inspect column names
-- when designing tables. Not used by any COPY INTO.
CREATE OR REPLACE FILE FORMAT FF_CSV_PEEK
    TYPE = CSV
    SKIP_HEADER = 0
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    COMMENT = 'Inspection only: keeps header row visible for schema discovery';


-- ---- External stages (one per source prefix) ------------------------------
CREATE OR REPLACE STAGE STAGE_SBA_7A
    STORAGE_INTEGRATION = SBLI_S3_INTEGRATION
    URL = 's3://sbli-platform-raw-dt/sba-7a-raw/'
    FILE_FORMAT = FF_CSV_STANDARD
    COMMENT = 'External stage for SBA 7(a) FOIA loan files';

CREATE OR REPLACE STAGE STAGE_SBA_504
    STORAGE_INTEGRATION = SBLI_S3_INTEGRATION
    URL = 's3://sbli-platform-raw-dt/sba-504-raw/'
    FILE_FORMAT = FF_CSV_STANDARD
    COMMENT = 'External stage for SBA 504 FOIA loan files';

CREATE OR REPLACE STAGE STAGE_FRED
    STORAGE_INTEGRATION = SBLI_S3_INTEGRATION
    URL = 's3://sbli-platform-raw-dt/fred-data-raw/'
    FILE_FORMAT = FF_JSON_STANDARD
    COMMENT = 'External stage for FRED API JSON responses';

CREATE OR REPLACE STAGE STAGE_BLS
    STORAGE_INTEGRATION = SBLI_S3_INTEGRATION
    URL = 's3://sbli-platform-raw-dt/bls-data-raw/'
    FILE_FORMAT = FF_JSON_STANDARD
    COMMENT = 'External stage for BLS API JSON responses';

CREATE OR REPLACE STAGE STAGE_CENSUS
    STORAGE_INTEGRATION = SBLI_S3_INTEGRATION
    URL = 's3://sbli-platform-raw-dt/census-data-raw/'
    FILE_FORMAT = FF_CSV_STANDARD
    COMMENT = 'External stage for Census BFS CSV files';


-- ============================================================================
-- VALIDATION  (read-only — confirms the build is correct)
-- ============================================================================

-- Foundation objects exist
SHOW WAREHOUSES LIKE 'SBLI_WH';
SHOW SCHEMAS IN DATABASE SBLI;
SHOW ROLES LIKE 'SBLI_ROLE';

-- Integration handshake values — copy STORAGE_AWS_IAM_USER_ARN and
-- STORAGE_AWS_EXTERNAL_ID into the AWS IAM trust policy.
DESC INTEGRATION SBLI_S3_INTEGRATION;

-- File formats and stages exist
SHOW FILE FORMATS IN SCHEMA SBLI.RAW;
SHOW STAGES IN SCHEMA SBLI.RAW;

-- S3 connectivity — each LIST should return files (or empty, but no error)
LIST @STAGE_SBA_7A;
LIST @STAGE_SBA_504;
LIST @STAGE_FRED;
LIST @STAGE_BLS;
LIST @STAGE_CENSUS;