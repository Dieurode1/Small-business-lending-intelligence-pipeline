"""
Pull BLS series, land raw JSON in S3.
One POST returns all series; split into one file per series for consistency
with the FRED pattern.

Run manually for now. Will be wrapped as a Dagster asset later.
"""
import os
import json
import requests
import boto3
from datetime import datetime, UTC
from dotenv import load_dotenv

load_dotenv()

BLS_API_KEY = os.getenv("BLS_API_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = "bls-data-raw"

# BLS caps requests at 20 years and silently truncates from the end if you exceed.
# Anchor the window to (current_year - 19) → current_year to guarantee recent data.
END_YEAR = str(datetime.now(UTC).year)
START_YEAR = str(int(END_YEAR) - 19)

SERIES = [
    "LNS14000000",            # Unemployment Rate, national, SA
    "CES0000000001",          # Total Nonfarm Employment
    "CES0500000003",          # Average Hourly Earnings, total private
    "CUUR0000SA0",            # CPI-U, all items, US city avg (NSA)
    "LNS11300000",            # Labor Force Participation Rate
    "JTS000000000000000JOL",  # Job Openings, total nonfarm (JOLTS)
]

def fetch_all_series(series_ids):
    """One POST returns all series; return dict keyed by series ID."""
    url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    body = {
        "seriesid": series_ids,
        "startyear": START_YEAR,
        "endyear": END_YEAR,
        "registrationkey": BLS_API_KEY,
    }
    r = requests.post(url, json=body, timeout=60)
    r.raise_for_status()
    response = r.json()

    if response.get("status") != "REQUEST_SUCCEEDED":
        raise RuntimeError(f"BLS API error: {response.get('message')}")

    # BLS returns warnings even on success (truncated ranges, missing years, etc.).
    # Print them loudly so silent truncation never reaches downstream stages.
    for msg in response.get("message", []):
        print(f"  ⚠ BLS message: {msg}")

    extracted_at = datetime.now(UTC).isoformat()
    by_series = {}
    for s in response["Results"]["series"]:
        sid = s["seriesID"]
        by_series[sid] = {
            "series_id": sid,
            "extracted_at": extracted_at,
            "data": s.get("data", []),
        }
    return by_series

def upload_to_s3(series_id, payload):
    s3 = boto3.client("s3")
    date_str = datetime.now(UTC).strftime("%Y%m%d")
    key = f"{S3_PREFIX}/series_{series_id.lower()}_{date_str}.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(payload),
        ContentType="application/json"
    )
    print(f"  → s3://{S3_BUCKET}/{key}")

def main():
    print(f"Pulling {len(SERIES)} BLS series ({START_YEAR}–{END_YEAR})...")
    payloads = fetch_all_series(SERIES)
    for series_id in SERIES:
        if series_id not in payloads:
            print(f"  ! {series_id} missing from response")
            continue
        print(f"  {series_id}")
        upload_to_s3(series_id, payloads[series_id])
    print("Done.")

if __name__ == "__main__":
    main()