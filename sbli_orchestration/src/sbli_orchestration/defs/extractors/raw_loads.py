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


def _run_load(context, snowflake, sql_filename, raw_tables):
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
    total = sum(counts.values())
    metadata = {"sql_file": sql_filename, "total_rows_loaded": total}
    metadata.update({f"rows__{t}": n for t, n in counts.items()})
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(key="fred_raw_load", group_name="raw_load", deps=["fred_extract"],
          compute_kind="snowflake",
          description="Load FRED JSON from S3 into SBLI.RAW.FRED_SERIES_RAW.")
def fred_raw_load(context, snowflake: SnowflakeResource) -> dg.MaterializeResult:
    return _run_load(context, snowflake, "05_sbli_fred.sql", ["FRED_SERIES_RAW"])


@dg.asset(key="bls_raw_load", group_name="raw_load", deps=["bls_extract"],
          compute_kind="snowflake",
          description="Load BLS JSON from S3 into SBLI.RAW.BLS_SERIES_RAW.")
def bls_raw_load(context, snowflake: SnowflakeResource) -> dg.MaterializeResult:
    return _run_load(context, snowflake, "06_sbli_bls.sql", ["BLS_SERIES_RAW"])


@dg.asset(key="census_raw_load", group_name="raw_load", deps=["census_extract"],
          compute_kind="snowflake",
          description="Load Census BFS CSVs from S3 into 4 RAW tables.")
def census_raw_load(context, snowflake: SnowflakeResource) -> dg.MaterializeResult:
    return _run_load(context, snowflake, "04_sbli_census_bfs.sql",
                     ["CENSUS_BFS_STATE_APPS_WEEKLY",
                      "CENSUS_BFS_REGION_APPS_WEEKLY",
                      "CENSUS_BFS_US_APPS_WEEKLY",
                      "CENSUS_BFS_DATE_TABLE"])


@dg.asset(key="sba_7a_raw_load", group_name="raw_load", deps=["sba_extract"],
          compute_kind="snowflake",
          description="Load SBA 7(a) FOIA CSVs from S3 into SBLI.RAW.SBA_7A_LOANS.")
def sba_7a_raw_load(context, snowflake: SnowflakeResource) -> dg.MaterializeResult:
    return _run_load(context, snowflake, "02_sbli_sba_7a.sql", ["SBA_7A_LOANS"])


@dg.asset(key="sba_504_raw_load", group_name="raw_load", deps=["sba_extract"],
          compute_kind="snowflake",
          description="Load SBA 504 FOIA CSVs from S3 into SBLI.RAW.SBA_504_LOANS.")
def sba_504_raw_load(context, snowflake: SnowflakeResource) -> dg.MaterializeResult:
    return _run_load(context, snowflake, "03_sbli_sba_504.sql", ["SBA_504_LOANS"])
