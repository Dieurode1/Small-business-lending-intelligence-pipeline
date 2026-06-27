"""
Extractor assets — wrap the existing Python extractors as Dagster assets.

Each extractor lives in the dbt project root (one level up from this
orchestration project) and writes raw source files to S3. These assets
represent "pull from API/source, land raw files in S3" — they do NOT load
to Snowflake RAW (that's a separate downstream asset, added next stage).
This keeps each asset single-responsibility and matches what the extractor
scripts do.

Two environment concerns handled here:
  1. The extractor scripts live in the dbt project root, not importable by
     default from this project — so we add that dir to sys.path.
  2. The extractors call load_dotenv() expecting .env in the cwd; when run
     from Dagster the cwd differs, so we load the dbt-root .env explicitly
     before invoking each extractor.
"""
import importlib
import sys
from pathlib import Path

from dotenv import load_dotenv

import dagster as dg

# The dbt project root (one level up from this orchestration project),
# where the extractor scripts and the .env file live.
EXTRACTOR_DIR = Path(__file__).resolve().parents[5]


def _prepare_extractor_env():
    """Make extractors importable and load their .env. Idempotent."""
    if str(EXTRACTOR_DIR) not in sys.path:
        sys.path.insert(0, str(EXTRACTOR_DIR))
    load_dotenv(EXTRACTOR_DIR / ".env")


@dg.asset(
    key="fred_extract",
    group_name="extractors",
    compute_kind="python",
    description="Pull 8 FRED economic series and land raw JSON in S3 "
    "(fred-data-raw/series_*.json). Envelope preserved.",
)
def fred_extract(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    _prepare_extractor_env()
    import fred

    context.log.info(f"Running FRED extractor for {len(fred.SERIES)} series")
    fred.main()

    return dg.MaterializeResult(
        metadata={
            "series_count": len(fred.SERIES),
            "s3_prefix": fred.S3_PREFIX,
            "series": ", ".join(fred.SERIES),
        }
    )


@dg.asset(
    key="bls_extract",
    group_name="extractors",
    compute_kind="python",
    description="Pull 6 BLS series (one POST, split per series) and land raw "
    "JSON in S3 (bls-data-raw/series_*.json). Envelope pre-unwrapped.",
)
def bls_extract(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    _prepare_extractor_env()
    import bls

    context.log.info(
        f"Running BLS extractor for {len(bls.SERIES)} series "
        f"({bls.START_YEAR}-{bls.END_YEAR})"
    )
    bls.main()

    return dg.MaterializeResult(
        metadata={
            "series_count": len(bls.SERIES),
            "s3_prefix": bls.S3_PREFIX,
            "year_range": f"{bls.START_YEAR}-{bls.END_YEAR}",
            "series": ", ".join(bls.SERIES),
        }
    )


@dg.asset(
    key="census_extract",
    group_name="extractors",
    compute_kind="python",
    description="Pull 4 Census BFS weekly CSVs (US/region/state apps + date "
    "table) and land raw in S3 (census-data-raw/*.csv). NSA only.",
)
def census_extract(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    _prepare_extractor_env()
    import census

    context.log.info(f"Running Census extractor for {len(census.FILES)} files")
    census.main()

    return dg.MaterializeResult(
        metadata={
            "file_count": len(census.FILES),
            "s3_prefix": census.S3_PREFIX,
            "files": ", ".join(stem for stem, _ in census.FILES),
        }
    )


@dg.asset(
    key="sba_extract",
    group_name="extractors",
    compute_kind="python",
    description="Pull SBA 7(a)+504 FOIA loan CSVs (6 large files + data "
    "dictionary) and land raw in S3. LARGE: hundreds of MB, several minutes.",
)
def sba_extract(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    _prepare_extractor_env()
    import sba

    context.log.info(
        f"Running SBA extractor for {len(sba.FILES)} files (asof {sba.ASOF}) "
        "— large download, may take several minutes"
    )
    sba.main()

    return dg.MaterializeResult(
        metadata={
            "file_count": len(sba.FILES),
            "asof": sba.ASOF,
            "s3_prefixes": f"{sba.S3_PREFIX_7A}, {sba.S3_PREFIX_504}, {sba.S3_PREFIX_DICT}",
        }
    )
