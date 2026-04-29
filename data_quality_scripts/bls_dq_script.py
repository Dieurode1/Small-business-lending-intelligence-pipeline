"""
Lightweight DQ check for BLS raw JSON in S3.
Confirms files landed and aren't garbage. Heavy validation lives in dbt.

Run after bls.py:
    python bls_dq_script.py
"""
import os
import json
import sys
import boto3
from datetime import datetime, date, UTC
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = "bls-data-raw"

# Max acceptable age (days) of latest observation per series.
# BLS publishes most series ~3-6 weeks after the reference month.
# JOLTS publishes with longer lag; thresholds calibrated empirically.
SERIES = {
    "LNS14000000":            75,   # Unemployment, monthly
    "CES0000000001":          75,   # Nonfarm employment, monthly
    "CES0500000003":          75,   # Avg hourly earnings, monthly
    "CUUR0000SA0":            75,   # CPI-U, monthly
    "LNS11300000":            75,   # Labor force participation, monthly
    "JTS000000000000000JOL":  90,   # JOLTS publishes with bigger lag
}

# BLS encodes period as M01–M13 (M13 = annual avg). We want monthly only.
def parse_bls_date(year, period):
    """Convert BLS year+period (e.g. '2026', 'M03') to a date."""
    if not period.startswith("M") or period == "M13":
        return None
    month = int(period[1:])
    return date(int(year), month, 1)

def check_series(s3, series_id, max_age_days, run_date):
    """Return (passed: bool, message: str)."""
    key = f"{S3_PREFIX}/series_{series_id.lower()}_{run_date:%Y%m%d}.json"
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        payload = json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        return False, "file missing for today's run"
    except json.JSONDecodeError:
        return False, "file is not valid JSON"

    if payload.get("series_id") != series_id:
        return False, f"series_id mismatch (got {payload.get('series_id')})"

    data = payload.get("data") or []
    if not data:
        return False, "no observations in payload"

    # Find the latest valid monthly observation
    obs_dates = [parse_bls_date(d["year"], d["period"]) for d in data]
    obs_dates = [d for d in obs_dates if d is not None]
    if not obs_dates:
        return False, "no monthly observations found"

    latest = max(obs_dates)
    age_days = (run_date - latest).days
    if age_days > max_age_days:
        return False, f"stale — latest obs {latest} ({age_days}d old, max {max_age_days}d)"

    return True, f"{len(data)} obs, latest {latest} ({age_days}d old)"

def main():
    s3 = boto3.client("s3")
    run_date = datetime.now(UTC).date()
    print(f"BLS DQ check — {run_date}\n" + "=" * 50)

    failures = 0
    for series_id, max_age in SERIES.items():
        passed, msg = check_series(s3, series_id, max_age, run_date)
        status = "PASS" if passed else "FAIL"
        print(f"{series_id:<28} [{status}] {msg}")
        if not passed:
            failures += 1

    print("=" * 50)
    print(f"{len(SERIES) - failures}/{len(SERIES)} passed")
    sys.exit(1 if failures else 0)

if __name__ == "__main__":
    main()