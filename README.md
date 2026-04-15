# Small Business Lending Intelligence Platform

> An end-to-end data engineering project that turns public SBA loan data and federal economic indicators into a queryable market intelligence layer for small business lending.

![Stack](https://img.shields.io/badge/stack-AWS%20%7C%20Snowflake%20%7C%20dbt%20%7C%20Dagster-blue)
![Status](https://img.shields.io/badge/status-in%20development-yellow)
![License](https://img.shields.io/badge/license-MIT-green)

---

## Overview

This project ingests **SBA 7(a) and 504 loan-level data** alongside **federal economic indicators** (FRED, BLS, Census) and transforms them into an analytics-ready warehouse. The output is a multi-source data product that answers questions small business owners, lenders, and economic development teams actually care about:

- Which industries are getting funded in which regions?
- How do interest rate changes correlate with loan approval volumes?
- Where is business formation accelerating faster than lending activity (i.e., underserved markets)?
- Who are the dominant SBA lenders in a given metro or NAICS sector?

The project is built as a production-grade pipeline — not a notebook — with testing, incremental loads, partitioned orchestration, CI/CD, and observability baked in from the start.

---

## Tech Stack

| Layer | Tool |
|---|---|
| Cloud storage | AWS S3 |
| Warehouse | Snowflake |
| Transformation | dbt |
| Orchestration | Dagster |
| Ingestion | Fivetran (REST APIs + CSV) |
| BI / app layer | Streamlit |
| CI/CD | GitHub Actions, sqlfluff |

---

## Data Sources

| Source | Description | Refresh |
|---|---|---|
| SBA 7(a) & 504 FOIA | Loan-level records since 1991 (amounts, NAICS, lender, location, jobs) | Quarterly |
| FRED API | Federal Reserve economic time series (rates, CPI, unemployment, GDP) | Daily–quarterly |
| BLS Public Data API | Employment, wages, and CPI by metro and industry | Monthly |
| Census Business Formation Statistics | New business applications by state and industry | Weekly |
| Census County Business Patterns | Establishment counts and payroll by industry and county | Annual |

---

## Architecture

```
   ┌─────────────┐    ┌──────────┐    ┌─────────┐    ┌───────────┐    ┌─────────────┐    ┌───────────┐
   │ SBA / FRED  │───▶│ Fivetran │───▶│   S3    │───▶│ Snowflake │───▶│     dbt     │───▶│ Streamlit │
   │ BLS / Census│    │(connector)│    │ (raw)   │    │   (RAW)   │    │ (STG → MART)│    │   app     │
   └─────────────┘    └──────────┘    └─────────┘    └───────────┘    └─────────────┘    └───────────┘
                                                           ▲                  ▲
                                                           │                  │
                                                     ┌──────────┐             │
                                                     │ Dagster  │─────────────┘
                                                     │          │  orchestration,
                                                     └──────────┘  partitions, sensors
                        └──────────┘                         
```

---

## Project Structure

```
.
├── ingestion/         # Python extractors for SBA, FRED, BLS, Census
├── dagster_project/   # Assets, schedules, sensors, partitions
├── dbt_project/       # Staging, intermediate, mart models + tests
├── app/               # Streamlit "Market Pulse" app
├── .github/workflows/ # CI: lint, test, dbt build on PR
└── docs/              # Architecture notes, ADRs, data dictionary
```

---

## Roadmap

- [ ] **Phase 1** — SBA Quarterly Loader (S3 → Snowflake → dbt → Dagster)
- [ ] **Phase 2** — Production hardening (tests, incremental, CI/CD, observability)
- [ ] **Phase 3** — Market Intelligence layer + Streamlit app
- [ ] **Phase 4** — ML features, unstructured data, vector search
- [ ] **Phase 5** — Capstone polish + system design writeup

---

## License

MIT
