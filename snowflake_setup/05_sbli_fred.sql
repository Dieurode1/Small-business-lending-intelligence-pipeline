-- ============================================================================
-- 05_SBLI_FRED
-- FRED economic series: single VARIANT table + JSON load.
-- Depends on 01_sbli_foundation (SBLI_S3_INTEGRATION, STAGE_FRED,
-- FF_JSON_STANDARD must already exist).
-- ============================================================================
--
-- Source: FRED API JSON responses, one file per series:
--   fred-data-raw/series_<id>_<YYYYMMDD>.json   (8 series, ~20.6K obs total)
--
-- JSON PATTERN (differs fundamentally from the CSV sources):
--   - Table has ONE VARIANT column holding the entire API response.
--   - COPY INTO uses $1 = the whole document (not positional columns).
--   - Flattening into row-per-observation is the dbt STAGING layer's job,
--     NOT raw's. Raw preserves the full FRED envelope (metadata + the
--     observations array) so nothing is lost.
--
-- series_id and pulled_date are derived from the FILENAME, not the JSON:
--   - FRED's response doesn't echo series_id in a convenient top-level field.
--   - "pulled date" isn't in the response at all — it's our ingestion
--     metadata, and the filename is its authoritative source.
--
-- Series ingested (FRED code -> what it measures):
--   BUSLOANS        Commercial & industrial loans, all banks
--   CPIAUCSL        Consumer Price Index (inflation)
--   DGS10           10-year Treasury rate
--   DRTSCILM        Banks tightening loan standards (SLOOS)
--   FEDFUNDS        Federal funds rate
--   GDP             Gross Domestic Product
--   RECPROUSM156N   Recession probability
--   UNRATE          Unemployment rate
--
-- Contrast with BLS (06): FRED stores the FULL API envelope; bls.py
-- pre-unwraps its envelope before storing. Both are defensible; the
-- difference is documented so the asymmetry isn't mistaken for a bug.


-- ============================================================================
-- BUILD
-- ============================================================================

USE ROLE SBLI_ROLE;
USE WAREHOUSE SBLI_WH;
USE DATABASE SBLI;
USE SCHEMA RAW;

CREATE OR REPLACE TABLE FRED_SERIES_RAW (
    series_id      VARCHAR(50),      -- derived from filename, e.g. 'UNRATE'
    pulled_date    DATE,             -- derived from filename (snapshot date)
    raw_response   VARIANT,          -- entire FRED API response
    _loaded_at     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file   VARCHAR(500)
)
COMMENT = 'Raw FRED API responses. One row per (series, pull_date). VARIANT preserves full response for downstream flattening in dbt staging.';

COPY INTO FRED_SERIES_RAW (series_id, pulled_date, raw_response, _source_file)
FROM (
    SELECT
        -- 'fred-data-raw/series_unrate_20260428.json' -> 'UNRATE'
        UPPER(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '_', 2)),

        -- '...series_unrate_20260428.json' -> 2026-04-28
        TO_DATE(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '_', -1), '.', 1),
                'YYYYMMDD'),

        -- entire JSON document
        $1,

        METADATA$FILENAME
    FROM @STAGE_FRED
)
PATTERN = '.*series_.*\.json'
ON_ERROR = CONTINUE;


-- ============================================================================
-- VALIDATION  (read-only — confirms the load is correct)
-- ============================================================================

-- Structure discovery (how the VARIANT path was confirmed — kept for
-- reproducibility if FRED changes its response shape):
--   SELECT OBJECT_KEYS(raw_response) FROM FRED_SERIES_RAW LIMIT 1;
--   SELECT raw_response FROM FRED_SERIES_RAW LIMIT 1;

-- Row count — expect 8 (one row per series file).
SELECT COUNT(*) AS series_loaded FROM FRED_SERIES_RAW;

-- One row per series, filename parsed correctly.
SELECT series_id, pulled_date, _source_file
FROM FRED_SERIES_RAW
ORDER BY series_id;

-- Integrity check: the count FRED reports must equal the actual number of
-- elements in the observations array. Equality across all 8 rows proves
-- the JSON was preserved end-to-end with no truncation.
SELECT
    series_id,
    raw_response:count::NUMBER              AS fred_reported_count,
    ARRAY_SIZE(raw_response:observations)   AS actual_array_size,
    (raw_response:count::NUMBER
        = ARRAY_SIZE(raw_response:observations)) AS counts_match
FROM FRED_SERIES_RAW
ORDER BY series_id;

-- Spot-flatten one series to confirm observations are queryable. This is a
-- PREVIEW of what the dbt staging model will do — not part of the raw load.
SELECT
    f.series_id,
    obs.value:date::DATE    AS observation_date,
    obs.value:value::STRING AS observation_value
FROM FRED_SERIES_RAW f,
     LATERAL FLATTEN(input => f.raw_response:observations) obs
WHERE f.series_id = 'UNRATE'
ORDER BY observation_date DESC
LIMIT 10;