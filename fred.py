"""
Pull FRED series, land raw JSON in S3.

S3 keys use canonical (un-dated) filenames so each rerun overwrites the
previous file rather than accumulating snapshots. FRED's response envelope
is stored intact, so observations live at raw_response:observations
(contrast with BLS, which is pre-unwrapped to raw_response:data).

Run manually for now. Will be wrapped as a Dagster asset later.
"""
import os
import json
import requests
import boto3
from dotenv import load_dotenv

load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = "fred-data-raw"

SERIES = [
    "FEDFUNDS",      # Fed Funds Rate
    "DGS10",         # 10-Year Treasury
    "UNRATE",        # Unemployment Rate
    "GDP",           # Real GDP
    "CPIAUCSL",      # CPI
    "BUSLOANS",      # C&I Loans
    "DRTSCILM",      # Bank tightening to small firms
    "RECPROUSM156N"  # Recession probability
]

def fetch_series(series_id):
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json"
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()
    payload["series_id"] = series_id
    return payload

def upload_to_s3(series_id, payload):
    s3 = boto3.client("s3")
    key = f"{S3_PREFIX}/series_{series_id.lower()}.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(payload),
        ContentType="application/json"
    )
    print(f"  → s3://{S3_BUCKET}/{key}")

def main():
    print(f"Pulling {len(SERIES)} FRED series...")
    for series_id in SERIES:
        print(f"  {series_id}")
        payload = fetch_series(series_id)
        upload_to_s3(series_id, payload)
    print("Done.")

if __name__ == "__main__":
    main()