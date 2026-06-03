-- ============================================================================
-- 07_SBLI_VALIDATION
-- Cross-source raw-layer health check. Read-only — creates and loads
-- nothing. Run AFTER 01–06 to confirm the entire RAW layer is intact.
-- ============================================================================
--
-- Per-source validation lives in each source worksheet (02–06). This file
-- is the PORTFOLIO-LEVEL view: one place a reviewer (or future-you) runs to
-- answer "is the raw layer fully and correctly loaded?" in one screen.
--
-- Expected state after a clean full load (the numbers we verified during
-- ingestion — treat these as the regression baseline):
--
--   SBA_7A_LOANS                  ~1,947,093   (≈99.998% of parsed; grows
--                                                as SBA approves new loans)
--   SBA_504_LOANS                 ~227,403     (≈99.997% of parsed; grows
--                                                as SBA approves new loans)
--   CENSUS_BFS_STATE_APPS_WEEKLY  ~54,060      (100%; grows weekly)
--   CENSUS_BFS_REGION_APPS_WEEKLY ~4,240       (100%; grows weekly)
--   CENSUS_BFS_US_APPS_WEEKLY     ~1,060       (100%; grows weekly)
--   CENSUS_BFS_DATE_TABLE         ~1,095       (100%; static)
--   FRED_SERIES_RAW               8 series
--   BLS_SERIES_RAW                6 series
--   ------------------------------------------------------------
--   Total CSV rows ≈ 2,233,851 ; JSON: 14 series docs
--
-- Architecture note: filenames in S3 are canonical (no date suffix) and
-- each extractor rerun overwrites the previous file. Snowflake RAW holds
-- a single snapshot per source. Pull history is not preserved at the raw
-- layer; if needed later, dbt snapshots are the path.
--
-- Small quarantine on the two SBA tables is EXPECTED and bounded
-- (malformed state values rejected via ON_ERROR = CONTINUE). It is a
-- deliberate design choice, not a defect — see 02 / 03 for the rationale.


USE ROLE SBLI_ROLE;
USE WAREHOUSE SBLI_WH;
USE DATABASE SBLI;
USE SCHEMA RAW;


-- ----------------------------------------------------------------------------
-- 1. Inventory: every RAW table exists and its row count vs. baseline
-- ----------------------------------------------------------------------------
SELECT 'SBA_7A_LOANS'                  AS table_name, COUNT(*) AS row_count FROM SBA_7A_LOANS
UNION ALL
SELECT 'SBA_504_LOANS',                COUNT(*) FROM SBA_504_LOANS
UNION ALL
SELECT 'CENSUS_BFS_STATE_APPS_WEEKLY', COUNT(*) FROM CENSUS_BFS_STATE_APPS_WEEKLY
UNION ALL
SELECT 'CENSUS_BFS_REGION_APPS_WEEKLY',COUNT(*) FROM CENSUS_BFS_REGION_APPS_WEEKLY
UNION ALL
SELECT 'CENSUS_BFS_US_APPS_WEEKLY',    COUNT(*) FROM CENSUS_BFS_US_APPS_WEEKLY
UNION ALL
SELECT 'CENSUS_BFS_DATE_TABLE',        COUNT(*) FROM CENSUS_BFS_DATE_TABLE
UNION ALL
SELECT 'FRED_SERIES_RAW',              COUNT(*) FROM FRED_SERIES_RAW
UNION ALL
SELECT 'BLS_SERIES_RAW',               COUNT(*) FROM BLS_SERIES_RAW
ORDER BY table_name;


-- ----------------------------------------------------------------------------
-- 2. Object inventory — confirms the supporting objects from 01 are present
--    (1 integration, 2 file formats, 5 stages, 8 tables in RAW)
-- ----------------------------------------------------------------------------
SHOW INTEGRATIONS LIKE 'SBLI_S3_INTEGRATION';
SHOW FILE FORMATS IN SCHEMA SBLI.RAW;
SHOW STAGES IN SCHEMA SBLI.RAW;
SHOW TABLES IN SCHEMA SBLI.RAW;


-- ----------------------------------------------------------------------------
-- 3. SBA load-quality: every source file contributed, quarantine is bounded
-- ----------------------------------------------------------------------------
-- Each program should show all of its decade files, none with 0 rows.
SELECT '7A' AS program, _source_file, COUNT(*) AS rows_loaded
FROM SBA_7A_LOANS GROUP BY _source_file
UNION ALL
SELECT '504', _source_file, COUNT(*)
FROM SBA_504_LOANS GROUP BY _source_file
ORDER BY program, _source_file;

-- Quarantine magnitude — expect tens of rows total, not thousands.
-- A large number here means a systemic parse problem, not stray bad data.
SELECT
    'SBA_7A'  AS tbl,
    COUNT(*)  AS rows_with_malformed_state
FROM SBA_7A_LOANS
WHERE LENGTH(borrstate) > 2 OR LENGTH(bankstate) > 2
UNION ALL
SELECT
    'SBA_504',
    COUNT(*)
FROM SBA_504_LOANS
WHERE LENGTH(borrstate) > 2
   OR LENGTH(cdc_state) > 2
   OR LENGTH(thirdpartylender_state) > 2;


-- ----------------------------------------------------------------------------
-- 4. Census integrity: geography cardinality + inf-handled-as-NULL
-- ----------------------------------------------------------------------------
SELECT
    (SELECT COUNT(DISTINCT state)  FROM CENSUS_BFS_STATE_APPS_WEEKLY)  AS distinct_states,   -- ~51
    (SELECT COUNT(DISTINCT region) FROM CENSUS_BFS_REGION_APPS_WEEKLY) AS distinct_regions,  -- 4
    (SELECT COUNT(*)               FROM CENSUS_BFS_US_APPS_WEEKLY)     AS us_weeks,
    (SELECT MIN(year)              FROM CENSUS_BFS_DATE_TABLE)         AS bfs_first_year;     -- 2006

-- The former pandas 'inf' values must be NULL (correct undefined-YoY),
-- never a literal string and never an error.
SELECT
    COUNT(*)                     AS state_rows,
    COUNT(*) - COUNT(yy_cba_nsa) AS yy_cba_nulls   -- expect > 0, all clean
FROM CENSUS_BFS_STATE_APPS_WEEKLY;


-- ----------------------------------------------------------------------------
-- 5. JSON sources: doc counts + filename↔payload agreement
-- ----------------------------------------------------------------------------
-- FRED: FRED-reported count must equal observations array size (no truncation)
SELECT
    series_id,
    raw_response:count::NUMBER            AS fred_count,
    ARRAY_SIZE(raw_response:observations) AS array_size,
    (raw_response:count::NUMBER
        = ARRAY_SIZE(raw_response:observations)) AS counts_match
FROM FRED_SERIES_RAW
ORDER BY series_id;

-- BLS: filename-derived series_id must equal JSON-embedded series_id
SELECT
    series_id,
    (series_id = raw_response:series_id::STRING) AS series_id_matches,
    ARRAY_SIZE(raw_response:data)                AS observation_count
FROM BLS_SERIES_RAW
ORDER BY series_id;


-- ----------------------------------------------------------------------------
-- 6. One-line GO / NO-GO summary
--    All booleans should be TRUE on a healthy raw layer.
--    FRED/BLS assertions use COUNT(DISTINCT series_id) so they stay correct
--    regardless of pull-history strategy (currently single-snapshot, but
--    a future SCD-style approach would push raw counts above 8/6).
-- ----------------------------------------------------------------------------
SELECT
    (SELECT COUNT(*) FROM SBA_7A_LOANS)  > 1900000  AS sba_7a_ok,
    (SELECT COUNT(*) FROM SBA_504_LOANS) > 220000   AS sba_504_ok,
    (SELECT COUNT(*) FROM CENSUS_BFS_STATE_APPS_WEEKLY)  > 50000 AS census_state_ok,
    (SELECT COUNT(*) FROM CENSUS_BFS_REGION_APPS_WEEKLY) > 4000  AS census_region_ok,
    (SELECT COUNT(*) FROM CENSUS_BFS_US_APPS_WEEKLY)     > 1000  AS census_us_ok,
    (SELECT COUNT(*) FROM CENSUS_BFS_DATE_TABLE)         > 1000  AS census_date_ok,
    (SELECT COUNT(DISTINCT series_id) FROM FRED_SERIES_RAW) = 8  AS fred_ok,
    (SELECT COUNT(DISTINCT series_id) FROM BLS_SERIES_RAW)  = 6  AS bls_ok;