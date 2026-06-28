"""
Asset checks — wrap the existing S3 data-quality scripts as Dagster asset
checks, one per extractor asset.

Each DQ script validates the raw files an extractor landed in S3 (file
present, valid, non-empty, and — for FRED/BLS — not stale relative to an
empirically-calibrated per-series age threshold). Heavy column-level and
business-logic validation lives in dbt; these are lightweight "did the
extract land and is it sane" gates.

We call each DQ module's inner check function directly (check_series /
check_file) rather than its main(), because main() ends in sys.exit(),
which would crash the check process. The inner functions are side-effect
free and return (passed: bool, message: str), so they reuse cleanly.

blocking=True: a failed check will block downstream assets (the future
RAW-load asset in stage 3) from running, so bad extracts never reach the
warehouse. Until that downstream exists, the checks simply report.
"""
import boto3
from datetime import datetime, UTC

import dagster as dg

from .assets import _prepare_extractor_env


@dg.asset_check(asset="fred_extract", blocking=True)
def fred_dq(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    _prepare_extractor_env()
    import fred_dq_script as dq

    s3 = boto3.client("s3")
    run_date = datetime.now(UTC).date()

    results = {}
    failures = 0
    for series_id, max_age in dq.SERIES.items():
        passed, msg = dq.check_series(s3, series_id, max_age, run_date)
        results[series_id] = f"{'PASS' if passed else 'FAIL'} — {msg}"
        if not passed:
            failures += 1

    total = len(dq.SERIES)
    return dg.AssetCheckResult(
        passed=(failures == 0),
        metadata={
            "summary": f"{total - failures}/{total} series passed",
            **results,
        },
    )


@dg.asset_check(asset="bls_extract", blocking=True)
def bls_dq(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    _prepare_extractor_env()
    import bls_dq_script as dq

    s3 = boto3.client("s3")
    run_date = datetime.now(UTC).date()

    results = {}
    failures = 0
    for series_id, max_age in dq.SERIES.items():
        passed, msg = dq.check_series(s3, series_id, max_age, run_date)
        results[series_id] = f"{'PASS' if passed else 'FAIL'} — {msg}"
        if not passed:
            failures += 1

    total = len(dq.SERIES)
    return dg.AssetCheckResult(
        passed=(failures == 0),
        metadata={
            "summary": f"{total - failures}/{total} series passed",
            **results,
        },
    )


@dg.asset_check(asset="census_extract", blocking=True)
def census_dq(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    _prepare_extractor_env()
    import census_dq_script as dq

    s3 = boto3.client("s3")

    results = {}
    failures = 0
    for filename_stem, cfg in dq.FILES.items():
        passed, msg = dq.check_file(s3, filename_stem, cfg["min_rows"])
        results[filename_stem] = f"{'PASS' if passed else 'FAIL'} — {cfg['label']}: {msg}"
        if not passed:
            failures += 1

    total = len(dq.FILES)
    return dg.AssetCheckResult(
        passed=(failures == 0),
        metadata={
            "summary": f"{total - failures}/{total} files passed",
            **results,
        },
    )


@dg.asset_check(asset="sba_extract", blocking=True)
def sba_dq(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    _prepare_extractor_env()
    import sba_dq_script as dq

    s3 = boto3.client("s3")

    results = {}
    failures = 0
    for prefix_stem, cfg in dq.FILES.items():
        check_results = dq.check_file(s3, prefix_stem, cfg)
        # sba_dq_script.check_file returns a list of (name, passed, msg) tuples
        file_failures = [name for name, passed, _ in check_results if not passed]
        passed = len(file_failures) == 0
        detail = "; ".join(f"{name}: {msg}" for name, _, msg in check_results)
        results[cfg["label"]] = f"{'PASS' if passed else 'FAIL'} — {detail}"
        if not passed:
            failures += 1

    total = len(dq.FILES)
    return dg.AssetCheckResult(
        passed=(failures == 0),
        metadata={
            "summary": f"{total - failures}/{total} files passed",
            **results,
        },
    )
