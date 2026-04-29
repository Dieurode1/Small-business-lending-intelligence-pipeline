"""
Lightweight DQ check for FRED raw JSON in S3.
Confirms files landed and aren't garbage. Heavy validation lives in dbt.

Run after fred.py:
    python fred_dq_script.py
"""
import os
import json
import sys
import boto3
from datetime import datetime, date, UTC
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = "fred-data-raw"

# Max acceptable age (days) of latest observation per series.
# These reflect FRED's actual publication lag, NOT a wishful target.
# Calibrated empirically — see docs/data_sources.md for rationale.
SERIES = {
    "FEDFUNDS":       75,   # monthly, Fed H.15 release
    "DGS10":          7,    # daily, Treasury
    "UNRATE":         75,   # monthly, BLS Employment Situation
    "GDP":            240,  # quarterly, BEA — ~6mo lag is normal
    "CPIAUCSL":       75,   # monthly, BLS CPI release
    "BUSLOANS":       75,   # weekly H.8, but obs date lags
    "DRTSCILM":       150,  # quarterly SLOOS survey
    "RECPROUSM156N":  120,  # monthly, NY Fed recession probability
}

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

    obs = payload.get("observations") or []
    if not obs:
        return False, "no observations in payload"
    if payload.get("series_id") != series_id:
        return False, f"series_id mismatch (got {payload.get('series_id')})"

    latest = max(o["date"] for o in obs)
    age_days = (run_date - date.fromisoformat(latest)).days
    if age_days > max_age_days:
        return False, f"stale — latest obs {latest} ({age_days}d old, max {max_age_days}d)"

    return True, f"{len(obs)} obs, latest {latest} ({age_days}d old)"

def main():
    s3 = boto3.client("s3")
    run_date = datetime.now(UTC).date()
    print(f"FRED DQ check — {run_date}\n" + "=" * 40)

    failures = 0
    for series_id, max_age in SERIES.items():
        passed, msg = check_series(s3, series_id, max_age, run_date)
        status = "PASS" if passed else "FAIL"
        print(f"{series_id:<15} [{status}] {msg}")
        if not passed:
            failures += 1

    print("=" * 40)
    print(f"{len(SERIES) - failures}/{len(SERIES)} passed")
    sys.exit(1 if failures else 0)

if __name__ == "__main__":
    main()