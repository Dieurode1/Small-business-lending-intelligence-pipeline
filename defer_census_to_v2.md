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


API investigation checklist — Census BFS

Things I do when a public API rejects my query, in order, before I defer or pivot:

1. First thing — wrap the request so it prints status_code and response.text instead of crashing on raise_for_status(). The real error message lives in the body, not the traceback. I lost time chasing a JSONDecodeError that was actually a hidden 400.
2. Check that my API key is loading correctly and has the right length. I had an 82-character Census key that should have been 40 — pasted it twice into .env by accident. Every error after that was misdiagnosed until I caught it.
3. Confirm I'm hitting the right endpoint by pulling https://api.census.gov/data.json. Turned out the real path was /timeseries/eits/bfs, not what I'd guessed from skimming docs.
4. Search the catalog broadly for related datasets to make sure I'm not missing an alternate version. Confirmed there's only one BFS dataset — no hidden state-level twin.
5. Pull the dataset's geography.json. This tells me which geographic levels are actually queryable. Saved me from assuming state-level was supported.
6. Pull the dataset's variables.json. This is where I found the required parameters that nobody documented anywhere else — geo_level_code and category_code.
7. Probe individual variables via variables/{name}.json to see valid values and whether they're required.
8. When the metadata doesn't expose valid values, brute-force the most likely ones — TOTAL, US, STATE, ALL, etc. Census is bad about hiding these.
9. Test the smallest possible successful query first — one time period, one geography. Don't try to scale up until I have one row coming back cleanly.
10. Document what I tried and what each step proved. The dead ends matter as much as the wins — they keep me from repeating the same investigation in v2.

If I were doing this again from scratch, I'd run geography.json and variables.json before writing a single line of extractor code. Would have saved me from building around assumptions about parameters I didn't even know existed.





# ⚠️ UPDATE — 2026-05-02: This decision has been superseded

**Status changed:** Accepted → Superseded
**Updated:** 2026-05-02

I deferred Census on 04/28 because the Census Data API rejected every reasonable query for state-level BFS. On 05/02 I came back to it and found the actual answer: **Census publishes state-level BFS data as direct CSV downloads** on their website, not through the API.

## What I did

Built a `census.py` that downloads four CSVs straight from `census.gov/econ/bfs/csv/`:

- `bfs_us_apps_weekly_nsa.csv` — national weekly business applications
- `bfs_region_apps_weekly_nsa.csv` — 4 Census regions
- **`bfs_state_apps_weekly_nsa.csv`** — 51 geographies (50 states + DC), the data I originally wanted
- `date_table.csv` — week ID → date lookup

The state-level CSV has ~53,800 rows (51 geographies × ~1,055 weekly observations going back to 2004). DQ check passes 4/4.

## Why this works when the API didn't

The Census Data API at `/timeseries/eits/bfs` is a structured query interface that only exposes the national-level aggregation. The CSV files at `census.gov/econ/bfs/data/weekly.html` are the underlying published dataset, and they include the state and regional breakdowns as separate columns. Two different surfaces over related but not identical data.

## What changed about the v1 narrative

The original framing — "v1 covers macroeconomic context, v2 adds geographic fairness" — flips. With state-level BFS in v1, geographic-fairness analysis is now in scope: I can compare new business formation by state against SBA lending volume by borrower state. That was the original intent.

## What's still deferred

CBP (County Business Patterns) is still v2. The reasoning in the original ADR still applies — different ingestion shape (per-year snapshots, nested geography, NAICS hierarchy decisions, suppressed cells). It's a multi-session integration that earns its place after the warehouse exists.

## Lesson

The mistake on 04/28 wasn't deferring — it was deferring without exhausting the alternatives. The API was a dead end; the bulk CSV downloads were sitting there the whole time. **Next time a public-data API doesn't support what I need, I'll check for direct CSV/bulk download endpoints before writing the deferral.** Added this lesson to the API investigation playbook below.

---

# ADR 0001: Defer Census Data Sources to v2 *(superseded — see update above)*

**Status:** ~~Accepted~~ Superseded
**Date:** 2026-04-28

## What I was trying to do
[... rest of your original content stays exactly as it is ...]