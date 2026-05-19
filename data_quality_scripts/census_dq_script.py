"""
Lightweight DQ check for Census BFS CSVs in S3.
Confirms today's pull landed and the basic shape is sane.

Run after census.py:
    python census_dq_script.py
"""
import os
import io
import sys
import csv
import boto3
from datetime import datetime, UTC
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = "census-data-raw"

# Per-file expected minimums (sanity bounds, not exact counts).
# Headers should be present, plus at least this many data rows.
FILES = {
    "bfs_us_apps_weekly_nsa":      {"min_rows": 100,  "label": "national"},
    "bfs_region_apps_weekly_nsa":  {"min_rows": 400,  "label": "regional (4 regions)"},
    "bfs_state_apps_weekly_nsa":   {"min_rows": 5000, "label": "state (51 geographies)"},
    "bfs_date_table":              {"min_rows": 100,  "label": "date lookup"},
}

def check_file(s3, filename_stem, min_rows, run_date):
    """Return (passed: bool, message: str)."""
    key = f"{S3_PREFIX}/{filename_stem}_{run_date:%Y%m%d}.csv"
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        body = obj["Body"].read().decode("utf-8", errors="replace")
    except s3.exceptions.NoSuchKey:
        return False, "file missing for today's run"

    if len(body) < 100:
        return False, f"file too small ({len(body)} bytes)"

    # Parse to confirm it's actually CSV
    try:
        reader = csv.reader(io.StringIO(body))
        rows = list(reader)
    except Exception as e:
        return False, f"CSV parse error: {e}"

    if len(rows) < 2:
        return False, "file has no data rows"

    header = rows[0]
    data_rows = rows[1:]
    n_data = len(data_rows)

    if n_data < min_rows:
        return False, f"only {n_data} rows (expected ≥{min_rows})"

    return True, f"{n_data:,} rows, {len(header)} columns"

def main():
    s3 = boto3.client("s3")
    run_date = datetime.now(UTC).date()
    print(f"Census BFS DQ check — {run_date}\n" + "=" * 60)

    failures = 0
    for filename_stem, cfg in FILES.items():
        passed, msg = check_file(s3, filename_stem, cfg["min_rows"], run_date)
        status = "PASS" if passed else "FAIL"
        label = cfg["label"]
        print(f"{filename_stem:<32} [{status}] {label}: {msg}")
        if not passed:
            failures += 1

    print("=" * 60)
    print(f"{len(FILES) - failures}/{len(FILES)} passed")
    sys.exit(1 if failures else 0)

if __name__ == "__main__":
    main()