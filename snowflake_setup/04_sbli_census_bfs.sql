-- ============================================================================
-- 04_SBLI_CENSUS_BFS
-- Census Business Formation Statistics: four stage-referenced tables + loads.
-- Depends on 01_sbli_foundation (SBLI_S3_INTEGRATION, STAGE_CENSUS,
-- FF_CSV_STANDARD, FF_CSV_PEEK must already exist).
-- ============================================================================
--
-- Source: census.gov/econ/bfs direct CSV download (the Census API does NOT
-- expose state-level BFS data — see project ADR on the API→CSV pivot).
-- All series are NSA (Not Seasonally Adjusted): raw weekly counts, no
-- seasonal smoothing. Seasonal adjustment, if needed, belongs in dbt.
--
-- FOUR tables, one per source file, because RAW is 1:1 with source:
--   CENSUS_BFS_STATE_APPS_WEEKLY   — 51 geographies, 11 cols, ~53.8K rows
--   CENSUS_BFS_REGION_APPS_WEEKLY  — 4 census regions, 12 cols, ~4.2K rows
--   CENSUS_BFS_US_APPS_WEEKLY      — national, 10 cols, ~1.1K rows
--   CENSUS_BFS_DATE_TABLE          — week→date reference, 4 cols, ~1.1K rows
--
-- The three weekly fact tables share the BA/HBA/WBA/CBA measure family plus
-- YoY variants; they differ only in geography dimension. They are UNION-ed
-- with a geo_level column in the dbt INTERMEDIATE layer, NOT here.
--
-- Measure glossary:
--   BA  = Business Applications (total)
--   HBA = High-Propensity BA (likely to become employers)
--   WBA = BA With Planned Wages
--   CBA = Corporate BA
--   YY_*= year-over-year change of the same measure
--
-- The 'inf' values in YY_* columns (pandas div-by-zero when prior-year
-- count was 0) are handled by FF_CSV_STANDARD's NULL_IF — they load as
-- NULL, which is the mathematically correct value for undefined YoY.
--
-- DATE_TABLE renames source headers "Start date"/"End date" to
-- start_date/end_date — one of the few legitimate raw-layer renames,
-- because the source names are literally invalid SQL identifiers
-- (contain spaces) without quoting. Source date format is M/D/YYYY,
-- parsed by FF_CSV_STANDARD's DATE_FORMAT = 'AUTO'.


-- ============================================================================
-- BUILD
-- ============================================================================

USE ROLE SBLI_ROLE;
USE WAREHOUSE SBLI_WH;
USE DATABASE SBLI;
USE SCHEMA RAW;


-- ---- State level (11 columns) --------------------------------------------
CREATE OR REPLACE TABLE CENSUS_BFS_STATE_APPS_WEEKLY (
    year              NUMBER(4),
    week              NUMBER(2),
    state             VARCHAR(2),
    ba_nsa            NUMBER(10),
    hba_nsa           NUMBER(10),
    wba_nsa           NUMBER(10),
    cba_nsa           NUMBER(10),
    yy_ba_nsa         NUMBER(15,4),
    yy_hba_nsa        NUMBER(15,4),
    yy_wba_nsa        NUMBER(15,4),
    yy_cba_nsa        NUMBER(15,4),
    _loaded_at        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file      VARCHAR(500)
)
COMMENT = 'Raw Census BFS state-level weekly business applications, NSA. 1:1 with source CSV.';

COPY INTO CENSUS_BFS_STATE_APPS_WEEKLY (
    year, week, state,
    ba_nsa, hba_nsa, wba_nsa, cba_nsa,
    yy_ba_nsa, yy_hba_nsa, yy_wba_nsa, yy_cba_nsa,
    _source_file
)
FROM (
    SELECT
        $1, $2, $3,
        $4, $5, $6, $7,
        $8, $9, $10, $11,
        METADATA$FILENAME
    FROM @STAGE_CENSUS
)
PATTERN = '.*bfs_state_apps_weekly_nsa.*\.csv'
ON_ERROR = CONTINUE;




-- ---- Region level (12 columns: adds Fregion numeric code) ----------------
CREATE OR REPLACE TABLE CENSUS_BFS_REGION_APPS_WEEKLY (
    year              NUMBER(4),
    week              NUMBER(2),
    region            VARCHAR(5),    -- text code: NE / MW / S / W
    fregion           NUMBER(2),     -- numeric FIPS-style: 1 / 2 / 3 / 4
    ba_nsa            NUMBER(12),
    hba_nsa           NUMBER(12),
    wba_nsa           NUMBER(12),
    cba_nsa           NUMBER(12),
    yy_ba_nsa         NUMBER(15,4),
    yy_hba_nsa        NUMBER(15,4),
    yy_wba_nsa        NUMBER(15,4),
    yy_cba_nsa        NUMBER(15,4),
    _loaded_at        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file      VARCHAR(500)
)
COMMENT = 'Raw Census BFS regional weekly business applications, NSA. Four census regions (NE/MW/S/W).';

COPY INTO CENSUS_BFS_REGION_APPS_WEEKLY (
    year, week, region, fregion,
    ba_nsa, hba_nsa, wba_nsa, cba_nsa,
    yy_ba_nsa, yy_hba_nsa, yy_wba_nsa, yy_cba_nsa,
    _source_file
)
FROM (
    SELECT
        $1, $2, $3, $4,
        $5, $6, $7, $8,
        $9, $10, $11, $12,
        METADATA$FILENAME
    FROM @STAGE_CENSUS
)
PATTERN = '.*bfs_region_apps_weekly_nsa.*\.csv'
ON_ERROR = CONTINUE;





-- ---- US / national level (10 columns: no geography dimension) ------------
CREATE OR REPLACE TABLE CENSUS_BFS_US_APPS_WEEKLY (
    year              NUMBER(4),
    week              NUMBER(2),
    ba_nsa            NUMBER(12),
    hba_nsa           NUMBER(12),
    wba_nsa           NUMBER(12),
    cba_nsa           NUMBER(12),
    yy_ba_nsa         NUMBER(15,4),
    yy_hba_nsa        NUMBER(15,4),
    yy_wba_nsa        NUMBER(15,4),
    yy_cba_nsa        NUMBER(15,4),
    _loaded_at        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file      VARCHAR(500)
)
COMMENT = 'Raw Census BFS national weekly business applications, NSA. 1:1 with source CSV.';

COPY INTO CENSUS_BFS_US_APPS_WEEKLY (
    year, week,
    ba_nsa, hba_nsa, wba_nsa, cba_nsa,
    yy_ba_nsa, yy_hba_nsa, yy_wba_nsa, yy_cba_nsa,
    _source_file
)
FROM (
    SELECT
        $1, $2,
        $3, $4, $5, $6,
        $7, $8, $9, $10,
        METADATA$FILENAME
    FROM @STAGE_CENSUS
)
PATTERN = '.*bfs_us_apps_weekly_nsa.*\.csv'
ON_ERROR = CONTINUE;





-- ---- Date reference table (4 columns; renamed headers) -------------------
CREATE OR REPLACE TABLE CENSUS_BFS_DATE_TABLE (
    year              NUMBER(4),
    week              NUMBER(2),
    start_date        DATE,          -- source header "Start date" (had a space)
    end_date          DATE,          -- source header "End date"   (had a space)
    _loaded_at        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file      VARCHAR(500)
)
COMMENT = 'Census BFS week-to-date reference. Join key (year, week) for the three BFS weekly fact tables.';

COPY INTO CENSUS_BFS_DATE_TABLE (
    year, week, start_date, end_date,
    _source_file
)
FROM (
    SELECT
        $1, $2, $3, $4,
        METADATA$FILENAME
    FROM @STAGE_CENSUS
)
PATTERN = '.*bfs_date_table.*\.csv'
ON_ERROR = CONTINUE;


-- ============================================================================
-- VALIDATION  (read-only — confirms the loads are correct)
-- ============================================================================

-- Schema discovery (how each layout was derived — kept for reproducibility):
--   SELECT $1..$11 FROM @STAGE_CENSUS/bfs_state_apps_weekly_nsa_*.csv
--     (FILE_FORMAT => FF_CSV_PEEK) LIMIT 1;   -- etc. for each file

-- Row counts — expect:
--   STATE  ~53,805   REGION ~4,220   US ~1,055   DATE ~1,095
-- All four should be 100% loaded (no quarantine; inf handled as NULL).
SELECT 'STATE'  AS tbl, COUNT(*) AS rows FROM CENSUS_BFS_STATE_APPS_WEEKLY
UNION ALL
SELECT 'REGION', COUNT(*) FROM CENSUS_BFS_REGION_APPS_WEEKLY
UNION ALL
SELECT 'US',     COUNT(*) FROM CENSUS_BFS_US_APPS_WEEKLY
UNION ALL
SELECT 'DATE',   COUNT(*) FROM CENSUS_BFS_DATE_TABLE
ORDER BY tbl;

-- Geography sanity: state file should have ~51 distinct states,
-- region file exactly 4 regions, mapped consistently to fregion.
SELECT COUNT(DISTINCT state) AS distinct_states
FROM CENSUS_BFS_STATE_APPS_WEEKLY;

SELECT region, fregion, COUNT(*) AS weeks
FROM CENSUS_BFS_REGION_APPS_WEEKLY
GROUP BY region, fregion
ORDER BY fregion;

-- inf-handling proof: YoY columns should contain NULLs (the former 'inf'
-- values), not error out and not contain a literal 'inf'. A non-zero NULL
-- count here is the expected, correct outcome.
SELECT
    COUNT(*)                       AS total_rows,
    COUNT(yy_cba_nsa)              AS yy_cba_non_null,
    COUNT(*) - COUNT(yy_cba_nsa)   AS yy_cba_null_was_inf_or_blank
FROM CENSUS_BFS_STATE_APPS_WEEKLY;

-- Date table parsed correctly: weeks are 7-day Sun–Sat spans,
-- earliest year should be 2006.
SELECT MIN(year) AS first_year, MAX(year) AS last_year,
       MIN(start_date) AS earliest_start, MAX(end_date) AS latest_end
FROM CENSUS_BFS_DATE_TABLE;

-- Sample rows from each fact table
SELECT * FROM CENSUS_BFS_STATE_APPS_WEEKLY  LIMIT 5;
SELECT * FROM CENSUS_BFS_REGION_APPS_WEEKLY LIMIT 5;
SELECT * FROM CENSUS_BFS_US_APPS_WEEKLY     LIMIT 5;
SELECT * FROM CENSUS_BFS_DATE_TABLE         LIMIT 5;