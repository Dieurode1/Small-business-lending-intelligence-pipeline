"""
Lightweight DQ check for SBA FOIA loan-level CSVs in S3.
Uses S3 metadata + streaming reads to avoid downloading hundreds of MB.

Heavy validation (column-level constraints, business logic) lives in dbt.

Run after sba.py:
    python sba_dq_script.py
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
ASOF = "251231"

# Shared core columns present in BOTH 7(a) and 504 schemas.
# These are the analytical bedrock — borrower geography, loan amount, NAICS.
# Program-specific columns (banks vs CDCs, revolvers, etc.) get validated
# in dbt staging models per-program.
SHARED_CORE_COLUMNS = [
    "asofdate", "program",
    "borrname", "borrstate", "borrzip",
    "grossapproval", "approvaldate", "firstdisbursementdate",
    "naicscode", "naicsdescription",
    "projectstate", "projectcounty",
    "businesstype", "loanstatus", "jobssupported",
]

# Per-file expected sanity bounds.
FILES = {
    "sba-7a-raw/foia_7a_fy1991_fy1999": {
        "size_mb": (50, 250),
        "min_rows": 100_000,
        "label": "7(a) FY1991-FY1999",
    },
    "sba-7a-raw/foia_7a_fy2000_fy2009": {
        "size_mb": (150, 500),
        "min_rows": 200_000,
        "label": "7(a) FY2000-FY2009",
    },
    "sba-7a-raw/foia_7a_fy2010_fy2019": {
        "size_mb": (100, 400),
        "min_rows": 100_000,
        "label": "7(a) FY2010-FY2019",
    },
    "sba-7a-raw/foia_7a_fy2020_present": {
        "size_mb": (50, 300),
        "min_rows": 100_000,
        "label": "7(a) FY2020-Present",
    },
    "sba-504-raw/foia_504_fy1991_fy2009": {
        "size_mb": (15, 100),
        "min_rows": 30_000,
        "label": "504 FY1991-FY2009",
    },
    "sba-504-raw/foia_504_fy2010_present": {
        "size_mb": (20, 150),
        "min_rows": 30_000,
        "label": "504 FY2010-Present",
    },
}

def find_today_file(s3, prefix_stem, run_date):
    """Locate the file matching today's pull date. Returns the full S3 key or None."""
    date_str = run_date.strftime("%Y%m%d")
    expected_key = f"{prefix_stem}_asof_{ASOF}_pulled_{date_str}.csv"
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=expected_key)
        return expected_key
    except s3.exceptions.ClientError:
        return None

def get_file_size_mb(s3, key):
    """Get file size in MB without downloading."""
    obj = s3.head_object(Bucket=S3_BUCKET, Key=key)
    return obj["ContentLength"] / (1024 * 1024)

def stream_count_rows_and_check_header(s3, key, expected_cols):
    """Stream the file in chunks. Returns (row_count, header_columns, missing_cols)
    without holding the whole file in memory."""
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    body = obj["Body"]

    # Read header line by streaming bytes until we hit a newline
    buf = b""
    while b"\n" not in buf:
        chunk = body.read(8192)
        if not chunk:
            break
        buf += chunk
    if b"\n" not in buf:
        return 0, [], list(expected_cols)

    header_bytes, remainder = buf.split(b"\n", 1)
    header_line = header_bytes.decode("utf-8", errors="replace").strip()
    header = next(csv.reader(io.StringIO(header_line)))
    missing = [c for c in expected_cols if c not in header]

    # Count rows by streaming the rest
    row_count = remainder.count(b"\n")
    while True:
        chunk = body.read(1024 * 1024)
        if not chunk:
            break
        row_count += chunk.count(b"\n")

    return row_count, header, missing

def check_file(s3, prefix_stem, cfg, run_date):
    results = []

    key = find_today_file(s3, prefix_stem, run_date)
    if not key:
        results.append(("file_exists", False, "file missing for today's pull date"))
        return results
    results.append(("file_exists", True, "OK"))

    size_mb = get_file_size_mb(s3, key)
    min_size, max_size = cfg["size_mb"]
    size_ok = min_size <= size_mb <= max_size
    results.append((
        "file_size",
        size_ok,
        f"{size_mb:.1f} MB" if size_ok else f"{size_mb:.1f} MB (expected {min_size}-{max_size} MB)"
    ))

    try:
        row_count, header, missing = stream_count_rows_and_check_header(
            s3, key, SHARED_CORE_COLUMNS
        )
    except Exception as e:
        results.append(("readable", False, f"stream read failed: {e}"))
        return results
    results.append(("readable", True, f"{len(header)} columns parsed"))

    rows_ok = row_count >= cfg["min_rows"]
    results.append((
        "row_count",
        rows_ok,
        f"{row_count:,} rows" if rows_ok else f"only {row_count:,} rows (expected ≥{cfg['min_rows']:,})"
    ))

    schema_ok = not missing
    results.append((
        "schema",
        schema_ok,
        "OK (all shared core columns present)" if schema_ok else f"missing core columns: {missing}"
    ))

    return results

def main():
    s3 = boto3.client("s3")
    run_date = datetime.now(UTC).date()
    print(f"SBA FOIA DQ check — {run_date}\n" + "=" * 60)

    total_failures = 0
    for prefix_stem, cfg in FILES.items():
        results = check_file(s3, prefix_stem, cfg, run_date)
        passed_ct = sum(1 for _, p, _ in results if p)
        all_passed = passed_ct == len(results)
        status = "PASS" if all_passed else "FAIL"
        print(f"\n  {cfg['label']:<25} [{status}] ({passed_ct}/{len(results)} checks)")
        for name, passed, msg in results:
            mark = "✓" if passed else "✗"
            print(f"    {mark} {name:<14} {msg}")
        if not all_passed:
            total_failures += 1

    print("\n" + "=" * 60)
    print(f"{len(FILES) - total_failures}/{len(FILES)} files passed all checks")
    sys.exit(1 if total_failures else 0)

if __name__ == "__main__":
    main()