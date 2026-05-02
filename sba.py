"""
Pull SBA 7(a) and 504 FOIA loan-level CSVs from data.sba.gov.

Source: https://data.sba.gov/en/dataset/7-a-504-foia
Files are large (hundreds of MB each) and updated quarterly.
URL filenames include the "as of" date (currently 251231 = Dec 31, 2025).

Six CSV files plus the data dictionary:
  - 7(a) FY1991-FY1999  (uses "as-of" with dashes in URL)
  - 7(a) FY2000-FY2009  (uses "as-of")
  - 7(a) FY2010-FY2019  (uses "as-of")
  - 7(a) FY2020-Present (uses "as-of")
  - 504  FY1991-FY2009  (uses "asof" without dashes — yes, SBA is inconsistent)
  - 504  FY2010-Present (uses "asof")
  - Data dictionary     (XLSX)

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
S3_PREFIX_7A = "sba-7a-raw"
S3_PREFIX_504 = "sba-504-raw"
S3_PREFIX_DICT = "sba-data-dictionary"

# Current "as of" date suffix from SBA. Update when SBA publishes a new quarter.
# See https://data.sba.gov/en/dataset/7-a-504-foia for the latest filenames.
ASOF = "251231"

BASE_URL = "https://data.sba.gov/dataset/0ff8e8e9-b967-4f4e-987c-6ac78c575087/resource"

# (filename_stem, source_url, s3_prefix)
FILES = [
    # 7(a) loans — note: "as-of" with dashes
    (
        "foia_7a_fy1991_fy1999",
        f"{BASE_URL}/182e9421-ccee-4562-acb3-93b34fb695f2/download/foia-7a-fy1991-fy1999-as-of-{ASOF}.csv",
        S3_PREFIX_7A,
    ),
    (
        "foia_7a_fy2000_fy2009",
        f"{BASE_URL}/186eb176-b53e-4cbe-ab93-e5c4fb50197d/download/foia-7a-fy2000-fy2009-as-of-{ASOF}.csv",
        S3_PREFIX_7A,
    ),
    (
        "foia_7a_fy2010_fy2019",
        f"{BASE_URL}/3f838176-6060-44db-9c91-b4acafbcb28c/download/foia-7a-fy2010-fy2019-as-of-{ASOF}.csv",
        S3_PREFIX_7A,
    ),
    (
        "foia_7a_fy2020_present",
        f"{BASE_URL}/d67d3ccb-2002-4134-a288-481b51cd3479/download/foia-7a-fy2020-present-as-of-{ASOF}.csv",
        S3_PREFIX_7A,
    ),
    # 504 loans — note: "asof" WITHOUT dashes
    (
        "foia_504_fy1991_fy2009",
        f"{BASE_URL}/8854d636-599d-463f-a961-7dbdb3bab152/download/foia-504-fy1991-fy2009-asof-{ASOF}.csv",
        S3_PREFIX_504,
    ),
    (
        "foia_504_fy2010_present",
        f"{BASE_URL}/4ad7f0f1-9da6-4d90-8bdb-89a6f821a1a9/download/foia-504-fy2010-present-asof-{ASOF}.csv",
        S3_PREFIX_504,
    ),
    # Data dictionary
    (
        "foia_data_dictionary",
        f"{BASE_URL}/6898b986-a895-47b4-bb7e-c6b286b23a7b/download/7a_504_foia_data_dictionary.xlsx",
        S3_PREFIX_DICT,
    ),
]

def fetch_and_upload(url, filename_stem, s3_prefix):
    """Download from URL, upload to S3."""
    s3 = boto3.client("s3")
    date_str = datetime.now(UTC).strftime("%Y%m%d")
    ext = "xlsx" if "dictionary" in filename_stem else "csv"
    key = f"{s3_prefix}/{filename_stem}_asof_{ASOF}_pulled_{date_str}.{ext}"

    print(f"  downloading from {url[:80]}...")
    r = requests.get(url, timeout=300)
    r.raise_for_status()
    content = r.content
    size_mb = len(content) / (1024 * 1024)
    print(f"  → {size_mb:.1f} MB downloaded")

    content_type = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        if ext == "xlsx" else "text/csv"
    )
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=content,
        ContentType=content_type,
    )
    print(f"  → s3://{S3_BUCKET}/{key}")

def main():
    print(f"Pulling {len(FILES)} SBA FOIA files (asof {ASOF})...")
    print("(This will take several minutes; SBA files are large.)\n")

    failures = []
    for filename_stem, url, s3_prefix in FILES:
        print(f"  {filename_stem}")
        try:
            fetch_and_upload(url, filename_stem, s3_prefix)
        except Exception as e:
            print(f"    ✗ FAILED: {e}")
            failures.append(filename_stem)
        print()

    if failures:
        print(f"Done with {len(failures)} failures: {failures}")
        sys.exit(1)
    print("Done.")

if __name__ == "__main__":
    main()