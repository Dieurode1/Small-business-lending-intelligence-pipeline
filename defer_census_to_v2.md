# ADR 0001: Defer Census Data Sources to v2

**Status:** Accepted
**Date:** 2026-04-28

## What I was trying to do

My original plan had four data sources: SBA loan data, FRED, BLS, and two Census Bureau datasets — Business Formation Statistics (BFS) and County Business Patterns (CBP). The Census sources were meant to support geographic-fairness analysis: is SBA lending actually reaching the counties where new businesses are forming and existing businesses are concentrated?

## What I ran into

**BFS.** I went after state-level data first since that's what pairs cleanly with SBA's borrower geography. The Census API kept returning `unknown/unsupported geography hierarchy`. After pulling the dataset's `geography.json`, I confirmed that BFS at the `/timeseries/eits/bfs` endpoint only supports `us:*` — national totals. State-level BFS exists as a separate Census product but isn't accessible through this endpoint.

**CBP.** Different problem. CBP isn't a time series — it's annual snapshots, one URL per year, and the geography model is nested (you have to specify both a `for` and an `in` parameter, e.g. `for=county:*&in=state:01`). To get full county coverage I'd need to make 51 separate API calls and stitch the results together. On top of that, CBP returns NAICS-coded data at multiple hierarchy levels (2-digit through 6-digit), so I'd need to make a real product decision about granularity before even writing the script. And cells get suppressed for privacy when business counts are small, which means the DQ logic is more involved than what I built for FRED and BLS.

## What I decided

Defer both Census sources to v2. Build the warehouse and dbt mart layers first using SBA, FRED, and BLS.

## Why

1. The three remaining sources cover the v1 analytical story I actually want to tell: how SBA lending behaves across rate cycles, labor market regimes, and recessions. Census would have added a geographic-fairness angle, but it's a *different* analytical layer, not a prerequisite for the core story.
2. The biggest unknown in this project right now is whether the warehouse layer (Snowflake + dbt) actually works end-to-end. That's where my architectural risk lives — not in the count of ingestion sources. I'd rather de-risk the warehouse with three sources than have four sources stuck in S3.
3. CBP is honestly a stronger v2 dataset than v1 dataset. Once the SBA mart exists, I'll know exactly which CBP variables I need to enrich it (NAICS-by-county establishment counts to compare against loan flow). Adding it now means designing the integration around guesses; adding it later means designing it around real analytical questions.
4. National-only BFS as a placeholder didn't earn its place. It's a single time series that doesn't tell the geographic story it was supposed to tell, and adding it just to hit the four-source count would clutter the v1 narrative.

## Consequences

- v1 narrative reframed from "geographic-fairness analysis" to "macroeconomic context for SBA lending." Honestly more focused this way.
- v2 revisits Census with a clear analytical brief and a separate architecture pass for the per-year, per-state, NAICS-hierarchy shape.
- Keeping the empty `census-data-raw/` folder in S3 for future use. Removing the half-built `census.py` and `census_dq_script.py` from the repo — the Git history preserves the attempt if I need to look back.