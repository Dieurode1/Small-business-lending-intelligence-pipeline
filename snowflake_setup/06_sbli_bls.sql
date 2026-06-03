-- ============================================================================
-- 06_SBLI_BLS
-- BLS economic series: single VARIANT table + JSON load.
-- Depends on 01_sbli_foundation (SBLI_S3_INTEGRATION, STAGE_BLS,
-- FF_JSON_STANDARD must already exist).
-- ============================================================================
--
-- Source: BLS API responses, one file per series:
--   bls-data-raw/series_<id>.json   (6 series, ~1.4K obs total)
--
-- Filenames are canonical (no date suffix); each extractor rerun overwrites
-- the previous file. Pull timestamp lives inside the JSON payload via the
-- extracted_at field, and is the source for the pulled_date column below.
--
-- Same JSON/VARIANT pattern as 05_sbli_fred: one VARIANT column holds the
-- whole document; $1 = entire document; flattening happens in dbt staging.
-- series_id is derived from the FILENAME; pulled_date is derived from
-- the JSON's extracted_at field (more accurate than file mtime — it's the
-- exact moment the extractor hit the BLS API).
--
-- ⚠ IMPORTANT ENVELOPE DIFFERENCE FROM FRED (verified via OBJECT_KEYS):
--   FRED stores the FULL API envelope -> raw_response:observations
--   bls.py PRE-UNWRAPS the BLS envelope before writing to S3. The stored
--   top-level keys are:  data, extracted_at, series_id
--   So observations live at  raw_response:data  (NOT
--   raw_response:Results.series[0].data, which is the native BLS shape).
--   This asymmetry is a deliberate extractor design choice, not a bug.
--   It is documented here so future-you / a reviewer doesn't "fix" it.
--
-- Series ingested (BLS code -> what it measures):
--   CES0000000001            Total nonfarm employment
--   CES0500000003            Avg hourly earnings, private
--   CUUR0000SA0              CPI-U, all items (inflation)
--   JTS000000000000000JOL    Job openings (JOLTS)
--   LNS11300000              Labor force participation rate
--   LNS14000000              Unemployment rate
--
-- CPI and unemployment intentionally duplicate FRED series (different
-- source) for cross-source validation. JOLTS and participation are unique
-- to BLS. JOLTS lags ~1 month vs CES (release covers two months prior),
-- so its observation count is expected to be ~1 lower than the others.


-- ============================================================================
-- BUILD
-- ============================================================================

USE ROLE SBLI_ROLE;
USE WAREHOUSE SBLI_WH;
USE DATABASE SBLI;
USE SCHEMA RAW;

CREATE OR REPLACE TABLE BLS_SERIES_RAW (
    series_id      VARCHAR(50),      -- derived from filename, e.g. 'CES0000000001'
    pulled_date    DATE,             -- derived from JSON's extracted_at timestamp
    raw_response   VARIANT,          -- pre-unwrapped BLS doc: {data, extracted_at, series_id}
    _loaded_at     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file   VARCHAR(500)
)
COMMENT = 'Raw BLS API responses (envelope pre-unwrapped by bls.py). One row per series. Observations at raw_response:data.';

COPY INTO BLS_SERIES_RAW (series_id, pulled_date, raw_response, _source_file)
FROM (
    SELECT
        -- 'bls-data-raw/series_ces0000000001.json' -> 'CES0000000001'
        -- (strip path, strip 'series_' prefix, strip '.json' suffix)
        UPPER(SPLIT_PART(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 2), '.', 1)),

        -- pulled_date from the JSON's own extracted_at timestamp.
        -- More accurate than METADATA$FILE_LAST_MODIFIED — this is the
        -- exact moment the extractor hit the BLS API, before any S3 lag.
        $1:extracted_at::TIMESTAMP_NTZ::DATE,

        -- entire JSON document
        $1,

        METADATA$FILENAME
    FROM @STAGE_BLS
)
PATTERN = '.*series_.*\.json'
ON_ERROR = CONTINUE;

-- ============================================================================
-- VALIDATION  (read-only — confirms the load is correct)
-- ============================================================================

-- Structure discovery (this is how the envelope difference from FRED was
-- found — kept for reproducibility; the NULL-returning first attempt is
-- exactly why OBJECT_KEYS is the right first move on any VARIANT load):
--   SELECT OBJECT_KEYS(raw_response) FROM BLS_SERIES_RAW LIMIT 1;
--     -> ['data', 'extracted_at', 'series_id']  (NOT 'Results'/'status')
--   SELECT raw_response FROM BLS_SERIES_RAW LIMIT 1;

-- Row count — expect 6 (one row per series file).
SELECT COUNT(*) AS series_loaded FROM BLS_SERIES_RAW;

-- One row per series; filename-derived series_id should equal the
-- series_id embedded in the JSON (built-in cross-check), and pulled_date
-- should track extracted_at.
SELECT
    series_id,
    pulled_date,
    raw_response:series_id::STRING    AS json_series_id,
    raw_response:extracted_at::STRING AS json_extracted_at,
    ARRAY_SIZE(raw_response:data)     AS observation_count
FROM BLS_SERIES_RAW
ORDER BY series_id;

-- Integrity assertions, made explicit:
--   filename series_id MUST equal JSON series_id (parsing correctness)
--   observation_count should be ~231 for 5 series, ~230 for JOLTS (lag)
SELECT
    series_id,
    (series_id = raw_response:series_id::STRING) AS series_id_matches,
    ARRAY_SIZE(raw_response:data)                AS observation_count
FROM BLS_SERIES_RAW
ORDER BY series_id;

-- Spot-flatten one series — PREVIEW of the dbt staging logic, and proof
-- the observations are queryable out of the VARIANT. Note the path is
-- raw_response:data (the FRED model uses raw_response:observations —
-- the documented envelope difference in action).
SELECT
    b.series_id,
    obs.value:year::INTEGER       AS observation_year,
    obs.value:period::STRING      AS observation_period,
    obs.value:periodName::STRING  AS period_name,
    obs.value:value::STRING       AS observation_value
FROM BLS_SERIES_RAW b,
     LATERAL FLATTEN(input => b.raw_response:data) obs
WHERE b.series_id = 'LNS14000000'
ORDER BY observation_year DESC,
         observation_period DESC
LIMIT 12;

SELECT series_id, pulled_date, ARRAY_SIZE(raw_response:data) AS obs_count
FROM BLS_SERIES_RAW
ORDER BY series_id;