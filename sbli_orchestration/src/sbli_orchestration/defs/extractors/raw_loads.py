"""
RAW-load assets — load each source's S3 files into Snowflake RAW by running
the existing snowflake_setup/*.sql BUILD section (CREATE OR REPLACE TABLE +
COPY INTO). The COPY INTO pulls from S3 via the storage integration.
"""
import re
from pathlib import Path

import dagster as dg
from dagster_snowflake import SnowflakeResource

SETUP_DIR = Path(__file__).resolve().parents[5] / "snowflake_setup"
VALIDATION_MARKER = "-- VALIDATION"


def _build_statements(sql_filename: str) -> list[str]:
    text = (SETUP_DIR / sql_filename).read_text()
    idx = text.find(VALIDATION_MARKER)
    if idx != -1:
        text = text[:idx]
    no_comments = "\n".join(
        re.sub(r"--.*$", "", line) for line in text.splitlines()
    )
    statements = []
    for raw in no_comments.split(";"):
        stmt = raw.strip()
        if stmt:
            statements.append(stmt)
    return statements


def _execute_and_count(context, snowflake, sql_filename, raw_tables):
    """Run a setup file's BUILD section, return {table: row_count}."""
    statements = _build_statements(sql_filename)
    context.log.info(f"Running {len(statements)} statements from {sql_filename}")
    counts = {}
    with snowflake.get_connection() as conn:
        cur = conn.cursor()
        for stmt in statements:
            preview = " ".join(stmt.split())[:80]
            context.log.info(f"  executing: {preview}...")
            cur.execute(stmt)
        for table in raw_tables:
            cur.execute(f"SELECT COUNT(*) FROM SBLI.RAW.{table}")
            counts[table] = cur.fetchone()[0]
    for table, n in counts.items():
        context.log.info(f"{table}: {n:,} rows loaded")
    return counts


def _single_load(context, snowflake, sql_filename, raw_table):
    counts = _execute_and_count(context, snowflake, sql_filename, [raw_table])
    return dg.MaterializeResult(
        metadata={"raw_table": raw_table,
                  "rows_loaded": counts[raw_table],
                  "sql_file": sql_filename}
    )


@dg.asset(key="fred_raw_load", group_name="raw_load", deps=["fred_extract"],
          compute_kind="snowflake",
          description="Load FRED JSON from S3 into SBLI.RAW.FRED_SERIES_RAW.")
def fred_raw_load(context, snowflake: SnowflakeResource) -> dg.MaterializeResult:
    return _single_load(context, snowflake, "05_sbli_fred.sql", "FRED_SERIES_RAW")


@dg.asset(key="bls_raw_load", group_name="raw_load", deps=["bls_extract"],
          compute_kind="snowflake",
          description="Load BLS JSON from S3 into SBLI.RAW.BLS_SERIES_RAW.")
def bls_raw_load(context, snowflake: SnowflakeResource) -> dg.MaterializeResult:
    return _single_load(context, snowflake, "06_sbli_bls.sql", "BLS_SERIES_RAW")


@dg.asset(key="sba_7a_raw_load", group_name="raw_load", deps=["sba_extract"],
          compute_kind="snowflake",
          description="Load SBA 7(a) FOIA CSVs from S3 into SBLI.RAW.SBA_7A_LOANS.")
def sba_7a_raw_load(context, snowflake: SnowflakeResource) -> dg.MaterializeResult:
    return _single_load(context, snowflake, "02_sbli_sba_7a.sql", "SBA_7A_LOANS")


@dg.asset(key="sba_504_raw_load", group_name="raw_load", deps=["sba_extract"],
          compute_kind="snowflake",
          description="Load SBA 504 FOIA CSVs from S3 into SBLI.RAW.SBA_504_LOANS.")
def sba_504_raw_load(context, snowflake: SnowflakeResource) -> dg.MaterializeResult:
    return _single_load(context, snowflake, "03_sbli_sba_504.sql", "SBA_504_LOANS")


# Census loads four tables in one run of 04_sbli_census_bfs.sql, so it's a
# multi_asset that emits four distinct keys (one per table). Each dbt source
# maps to its own key, avoiding the duplicate-key collision a single shared
# key would cause.
_CENSUS_KEY_TO_TABLE = {
    "census_state_raw_load":  "CENSUS_BFS_STATE_APPS_WEEKLY",
    "census_region_raw_load": "CENSUS_BFS_REGION_APPS_WEEKLY",
    "census_us_raw_load":     "CENSUS_BFS_US_APPS_WEEKLY",
    "census_date_raw_load":   "CENSUS_BFS_DATE_TABLE",
}


@dg.multi_asset(
    group_name="raw_load",
    compute_kind="snowflake",
    specs=[
        dg.AssetSpec(key, deps=["census_extract"],
                     description=f"Load {table} from S3 via COPY INTO.")
        for key, table in _CENSUS_KEY_TO_TABLE.items()
    ],
)
def census_raw_load(context, snowflake: SnowflakeResource):
    counts = _execute_and_count(
        context, snowflake, "04_sbli_census_bfs.sql",
        list(_CENSUS_KEY_TO_TABLE.values()),
    )
    for key, table in _CENSUS_KEY_TO_TABLE.items():
        yield dg.MaterializeResult(
            asset_key=key,
            metadata={"raw_table": table,
                      "rows_loaded": counts[table],
                      "sql_file": "04_sbli_census_bfs.sql"},
        )
