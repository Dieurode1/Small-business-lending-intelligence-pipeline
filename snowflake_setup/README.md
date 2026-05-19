# Snowflake Setup

SQL to stand up the SBLI raw layer: warehouse, database, schemas, role,
S3 storage integration, file formats, external stages, and the nine RAW
tables that land data from four sources (SBA 7(a)/504, Census BFS, FRED,
BLS).

## Run order

Files are numbered in execution order. Run them top to bottom.

| File | Purpose |
|------|---------|
| `01_sbli_foundation.sql` | Shared infra: warehouse, DB, schemas, role, storage integration, file formats, external stages |
| `02_sbli_sba_7a.sql` | SBA 7(a) loan-level FOIA table + load |
| `03_sbli_sba_504.sql` | SBA 504 loan-level FOIA table + load |
| `04_sbli_census_bfs.sql` | Census Business Formation Statistics — 4 tables (state, region, US, date) + loads |
| `05_sbli_fred.sql` | FRED economic series — JSON/VARIANT table + load |
| `06_sbli_bls.sql` | BLS economic series — JSON/VARIANT table + load |
| `07_sbli_validation.sql` | Cross-source raw-layer health check (read-only) |

Each source file (`02`–`06`) is split internally into a **BUILD** section
(constructs and loads) and a **VALIDATION** section (read-only checks).
`07` is validation only and is safe to run anytime against a loaded
environment.

## ⚠ Placeholders — read before running

`01_sbli_foundation.sql` contains two environment-specific placeholders that
must be replaced locally before running:

- `<YOUR_SNOWFLAKE_USERNAME>` — your Snowflake login username
- `<YOUR_AWS_ROLE_ARN>` — the IAM role ARN for the S3 storage integration

These are intentionally left as placeholders so this repo can be public
without leaking an AWS account ID or Snowflake username. Fill them in at
run time; never commit the real values.

After creating the storage integration in `01`, run its `DESC INTEGRATION`
(in the validation section) and use the returned `STORAGE_AWS_IAM_USER_ARN`
and `STORAGE_AWS_EXTERNAL_ID` to update the AWS IAM trust policy. Stages
will not resolve until that trust handshake is complete.

## Idempotency

All objects use `CREATE OR REPLACE` / `CREATE ... IF NOT EXISTS`, so the
scripts can be re-run safely on an existing environment. Re-running a
source file drops and reloads that source's table(s) — intended behavior
for a clean rebuild.

## Notes

- Source files are loaded with `ON_ERROR = CONTINUE`. The two SBA tables
  carry a small, bounded quarantine (≈50 rows total) for malformed source
  state values — a deliberate design choice, documented in `02` / `03`.
- FRED stores the full API envelope; BLS is pre-unwrapped by `bls.py`.
  The path difference (`raw_response:observations` vs `raw_response:data`)
  is documented in `05` / `06`.