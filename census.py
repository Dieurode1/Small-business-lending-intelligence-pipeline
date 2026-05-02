"""
Pull Census Business Formation Statistics (BFS) weekly CSVs and land in S3.

Source: https://www.census.gov/econ/bfs/data/weekly.html
Data is published as static CSVs at stable URLs; updated as part of the
monthly BFS release cycle. NSA (not seasonally adjusted) only — the
seasonally adjusted series at state level isn't published.

Pulls four files:
  - National applications (weekly)
  - Regional applications (weekly, 4 Census regions)
  - State applications (weekly, 50 states + DC)
  - Date lookup table (maps week IDs to actual dates)

Run manually for now. Will be wrapped as a Dagster asset later.
"""
import os
import sys
import requests
import boto3
from datetime import datetime, UTC
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = "census-data-raw"

BFS_BASE_URL = "https://www.census.gov/econ/bfs/csv"

# (filename_stem, source_url) tuples
FILES = [
    ("bfs_us_apps_weekly_nsa",      f"{BFS_BASE_URL}/bfs_us_apps_weekly_nsa.csv"),
    ("bfs_region_apps_weekly_nsa",  f"{BFS_BASE_URL}/bfs_region_apps_weekly_nsa.csv"),
    ("bfs_state_apps_weekly_nsa",   f"{BFS_BASE_URL}/bfs_state_apps_weekly_nsa.csv"),
    ("bfs_date_table",              f"{BFS_BASE_URL}/date_table.csv"),
]

def fetch_csv(url):
    """Download a CSV and return its raw bytes."""
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content

def upload_to_s3(filename_stem, content):
    s3 = boto3.client("s3")
    date_str = datetime.now(UTC).strftime("%Y%m%d")
    key = f"{S3_PREFIX}/{filename_stem}_{date_str}.csv"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=content,
        ContentType="text/csv"
    )
    print(f"  → s3://{S3_BUCKET}/{key} ({len(content):,} bytes)")

def main():
    print(f"Pulling {len(FILES)} Census BFS CSVs...")
    failures = []
    for filename_stem, url in FILES:
        print(f"  {filename_stem}")
        try:
            content = fetch_csv(url)
            upload_to_s3(filename_stem, content)
        except Exception as e:
            print(f"    ✗ FAILED: {e}")
            failures.append(filename_stem)
    if failures:
        print(f"Done with {len(failures)} failures: {failures}")
        sys.exit(1)
    print("Done.")

if __name__ == "__main__":
    main()