# SBLI — How I Built This and Why

**Small Business Lending Intelligence pipeline**
Repo: `github.com/Dieurode1/Small-business-lending-intelligence-pipeline`

This is my notes on how this project is put together — not just what each piece
does, but why I made the calls I made. I wrote it so that if someone picks up the
repo cold (or honestly, if I come back to it in six months and forget everything),
they can get the full picture in a couple of minutes.

---

## 1. What it is and who I built it for

SBLI is an end-to-end data pipeline built on top of SBA (Small Business
Administration) lending data, with some macroeconomic data mixed in for context.
The whole point is to answer credit-risk and competitive questions: who's lending,
how those loans actually perform over time, and where the risk piles up — by
industry, by region, by lender.

Here's the flow:

```
Python extractors → AWS S3 → Snowflake RAW → dbt STAGING → INTERMEDIATE → MARTS
```

I built the analytical side (the marts) with three specific audiences in mind:

1. **SBA banks and CDCs** — so they can see how their own portfolio stacks up
   against everyone else.
2. **Fintech / BaaS lenders thinking about entering the SBA market** — so they can
   size up the competition before they jump in.
3. **Credit-risk analysts at big banks** — so they can study default patterns across
   the whole industry.

All three care about the same kinds of things: performance metrics, benchmarking
against peers, and figuring out where the risk is. None of them are consumer-facing.
That shaped every decision downstream — I leaned into default rates and risk
segmentation, not, say, helping a small business pick a lender.

Here's what I'm pulling from:

| Source | What it is | How often it updates | One row per... |
|---|---|---|---|
| SBA FOIA 7(a) | ~1.95M loans | quarterly | loan |
| SBA FOIA 504 | ~227K loans | quarterly | loan |
| FRED | 8 economic series | mixed (daily/monthly/quarterly) | series (stored as JSON) |
| BLS | 6 economic series | monthly | series (stored as JSON) |
| Census BFS | business applications | weekly | US / region / state |

---

## 2. Getting the data in (the ingestion layer)

### 2.1 I overwrite files in S3 instead of stacking them up

Each extractor writes to a fixed filename in S3 — `series_unrate.json`, not
`series_unrate_20260603.json`. Every time I rerun, it overwrites the old file. So
Snowflake's RAW layer always holds exactly one current snapshot per source.

I learned this the hard way. Originally I had dates in the filenames, which meant S3
kept piling up a new file every time I pulled. The `COPY INTO` was matching *all* of
them, so my row counts silently doubled on every rerun — I caught it when SBA 7(a)
jumped from ~1.9M to ~3.9M rows. There were a few places I could've fixed it, but I
went with the extractor layer because it keeps everything clean: the extractors
decide what "current" means, S3 just stores it, and Snowflake reads whatever's
there. No weird filtering logic spread across layers.

I did try fixing it inside `COPY INTO` first — filtering to the latest file with a
`WHERE METADATA$FILE_LAST_MODIFIED` subquery. Snowflake flat-out rejects that ("COPY
statement only supports simple SELECT from stage statements"). There's an
`EXECUTE IMMEDIATE` workaround but it makes every worksheet way more complicated for
no real benefit over the one-line fix in the extractor.

The trade-off I accepted: I don't keep any pull history. If a source revises a
number between pulls, the old value is just gone. That's fine for what this is — the
source APIs are always there to re-pull from. If I ever actually need to track
revisions, the right move is dbt Snapshots, not going back to dated filenames.

### 2.2 BLS and FRED store their JSON differently — on purpose

My BLS extractor strips out the API's outer wrapper before saving, so the actual
observations live at `raw_response:data`. My FRED extractor keeps the full response,
so its observations live at `raw_response:observations`.

I'm calling this out because it looks like a bug if you don't know it's intentional.
It's documented in both the RAW setup and the staging models so future-me doesn't
"fix" one to match the other.

### 2.3 The two sources figure out `pulled_date` differently

- **BLS** gets it from the `extracted_at` field inside the JSON — the exact moment I
  hit the API.
- **FRED** gets it from `METADATA$FILE_LAST_MODIFIED` (the S3 upload time), because
  FRED's response doesn't include a pull timestamp of its own.

Different sources, different best-available signal. I documented it so it reads as a
deliberate choice and not me being sloppy.

---

## 3. The staging layer

Eight staging models, one per RAW table. They're all views (cheap to rebuild, always
reflect the latest RAW). Same grain as the source — I'm just doing light cleanup
here: renaming columns, fixing types, handling missing-value markers. No joins, no
business logic yet.

### 3.1 SBA loans don't have a unique ID, so I made my own

This one surprised me. I assumed `l2locid` (7a) and `locationid` (504) were unique
loan IDs. They're not — there are only ~6,800 distinct `l2locid` values across 1.95M
rows. So I dug in. Even `(borrower_name, approval_date, amount)` together still left
~11K collisions.

Turns out it's not dirty data — it's real. Franchise and multi-location businesses
legitimately produce identical rows. Best example: "Meathead Movers" took out 20
separate $1,500 loans on the same day, one for each franchise location, all under
the same LLC. Twenty real, distinct loans that look identical on every field that
matters.

So I generate a surrogate key with `ROW_NUMBER()`, ordered by a stable set of fields
(approval date, borrower, amount, lender, NAICS, source file). It's guaranteed unique
because that's just how `ROW_NUMBER()` works. Everything downstream joins on that,
never on `l2locid`. The way I'd explain it in an interview: SBA's data has no unique
loan identifier because it reflects multi-location lending, so I built a deterministic
surrogate at staging and documented the limitation.

### 3.2 BLS has some gotchas

- `"-"` is BLS's marker for "no data," not zero. I have to NULL it before casting to
  a number, otherwise the cast blows up. It showed up in the 2025 government-shutdown
  gap.
- Period code `M13` is the annual average, not a real month — I filter it out since
  staging is monthly.
- The "preliminary" flag is buried in `footnotes[0].code = 'P'`, so I pull it up into
  its own boolean.

### 3.3 My first quarantine filter was too aggressive

I started with `WHERE length(state) = 2` to drop malformed rows — but that killed
~2,300 rows when only ~10 were actually broken (zip codes typed into the state
field). The rest were legit loans that just had a NULL state. Fixed it to
`(state IS NULL OR length(state) = 2)`, which keeps the NULLs and only drops the
genuinely corrupted rows. Now my staging count matches RAW exactly and the filter is
just there as a safety net.

### 3.4 Weird data I chose to surface instead of hide

- **~25 SBA 7(a) loans have no borrower name.** These are "EXEMPT FROM DISCLOSURE"
  loans — real banks, real amounts, the borrower's identity is just legally
  suppressed. I dropped the not-null test on borrower name and wrote down why.
- **One 1995 loan has a negative dollar amount** ("JOY ONE HOUR CLEANERS") — clearly
  some old data-entry mistake. Instead of dropping it, I set that range test to
  `warn` so it shows up as a flag without breaking the whole pipeline.

### 3.5 504 data is just cleaner than 7(a)

504 has zero NULL borrowers and zero negative amounts, so I can actually enforce
not-null on `borrower_name` and `cdc_name` there — assertions that would be wrong for
7(a). It says something about the two programs: 7(a) is older and way higher volume,
so it's accumulated a lot more edge cases. 504 is newer and more tightly run.

---

## 4. The intermediate layer

Four models, all views.

### 4.1 Combining two programs that don't match

`int_sba_loans_unified` is where I union 7(a) and 504 into one ~2.17M-row loan
table. The tricky part: the two programs have totally different lender structures.
7(a) is one bank. 504 is a CDC plus a third-party lender.

I went with keeping both generic *and* program-specific columns. Generic ones
(`primary_lender_name`, `lender_type`) for analysis that spans both programs, and the
specific ones (`bank_*`, `cdc_*`, `third_party_*`) kept around for drill-downs, even
though they're NULL on the other program. The NULLs actually tell you something —
if `cdc_name` is NULL, you know it's a 7(a) loan.

The surrogate key here is prefixed (`7A_` / `504_`) so it stays unique across the
combined table.

The hardest part of this whole model was normalizing loan status. The two programs
use completely different vocabularies — 7(a) has 13 cryptic codes (`P I F`,
`CHGOFF`, `CLSLN`...) and 504 has 24 mostly spelled-out ones (`PAID IN FULL`,
`1 MONTH PAST DUE` all the way through `>9 MONTHS PAST DUE`...). 37 distinct strings
total. I mapped them all to a clean two-level system: a granular `loan_status` (13
canonical values plus `UNKNOWN`), a coarser `loan_status_category` for high-level
stuff (COMPLETED_PAID, COMPLETED_LOSS, ACTIVE, INACTIVE, RESOLVED_OTHER, UNKNOWN),
and I kept the original string as `loan_status_raw` for audit.

### 4.2 The CLSLN thing (my favorite finding)

There's a 7(a) status code `CLSLN` — 75,735 rows. I almost mapped it to "not
funded." But I ran a quick check first: **99.96% of those rows have a charge-off
date, none have a paid-in-full date, and all of them were funded.** So it's a
charge-off wearing a disguise ("Closed Loan" — written off and closed out).

Mapping it correctly bumped my measured 7(a) charge-off count from 143,099 to
218,834. That's a **54% jump.** Which means SBA's headline charge-off rate (~7%)
actually undercounts the real charge-offs (~11%) by about half, because it doesn't
include these "closed" write-offs. If I had to pick one finding from this whole
project to talk about, it's this one: I found that SBA's reported charge-off rates
undercount actual losses by roughly half.

### 4.3 Why I track SBA purchases separately

There's a status, `SBA_PURCHASED` (source code `PURCH(NOT C/O)`), that means SBA
bought the loan guarantee from the bank — basically the loan went bad and the bank
cashed in the guarantee, but it's not formally charged off yet and SBA might still
recover some money. Economically it's a default, but it's its own thing.

I gave it its own boolean flag instead of lumping it in with charge-offs, because the
real credit-risk number people want is `(charged_off + sba_purchased) / total`. A
lender with a lot of SBA purchases is making bad loans even if their official
charge-off rate looks fine. That flag is the whole reason the lender-performance mart
can show a "true default rate." (In the coarse rollup I put it under
`RESOLVED_OTHER`, not `COMPLETED_LOSS`, so the categories stay honest about what the
data literally says — but analysts can still combine the two.)

### 4.4 Census: three models, and I check the join

`int_us_apps_with_dates`, `int_region_apps_with_dates`, and
`int_state_apps_with_dates` each join a weekly Census fact table to the date lookup.
I kept them as three separate models instead of cramming them into one, because they
have different grains and `FROM int_state_apps_with_dates` just reads clearer than
filtering one giant table by a geography level. I used an INNER JOIN and added a
row-count check to make sure no rows got dropped — they didn't, every week has a
matching date entry.

### 4.5 Why I haven't combined BLS and FRED yet

I deliberately didn't build a unified economic-indicators model. BLS is all monthly,
but FRED is a mix of daily, monthly, and quarterly. There's no single right way to
mash them together — do I resample everything to monthly? quarterly? keep the
cadences separate? It completely depends on what a downstream mart actually needs.
My rule is to build intermediate models for real consumers, not on a guess. A model
with nothing using it shouldn't exist yet. I'll build this one when Mart 4 tells me
exactly what shape it needs.

---

## 5. The marts (the actual answers)

Three marts, all materialized as tables since people query them constantly and it's
better to compute once than recompute every time. They all share the same metrics
and the same vintage framing.

### 5.1 Vintage framing (this applies to all three)

Each row is one dimension per **approval fiscal year** — basically "loans
*originated* in FY[X]," and the performance numbers reflect how those loans
*eventually* turned out. Banks call this vintage analysis. It's not a snapshot of the
current portfolio.

The big caveat I documented everywhere: recent vintages (FY2024+) have garbage
default rates because the loans haven't had time to go bad yet. SBA loans run 7–25
years; a loan approved a few months ago obviously hasn't defaulted. So any real
analysis filters to vintages at least 5–7 years old. A 0.5% default rate on FY2025
loans would be totally misleading — those loans are six months old.

### 5.2 The headline number: true default rate

`(charged_off + sba_purchased) / total_loans`. It's higher than the formal
charge-off rate because it includes SBA's guarantee payouts, and it's the number my
whole audience actually cares about. It only works because of the CLSLN fix (§4.2)
and the separate SBA-purchased flag (§4.3).

### 5.3 `fct_lender_performance`

One row per (lender, lender type, fiscal year), ~63K rows. I picked the lender's
"primary state" by dollar volume, not loan count, since market presence is really
about dollars. The data checks out against what I know about the industry —
Huntington as the volume leader, Live Oak with the lowest default rate and biggest
average loan, and the specialty non-banks like VelocitySBA and Celtic running
18–22% default rates.

### 5.4 `fct_loan_performance_by_industry` — why I didn't roll up

I built this at the full 6-digit NAICS level and added derived sector columns
(`naics_sector_code`, `naics_sector_name`) for rolling up. I thought about just
aggregating to the 2-digit sector at build time — it's cleaner and every bucket has
enough volume — but I decided against it.

The reasoning: **rolling up is easy and lossless, rolling down is impossible.** Once
you sum restaurants and bars and caterers into one "food services" number, you can
never get the breakdown back. So I keep the table at the finest level anyone might
need, and rolling up to sector is just a one-line `GROUP BY` at query time. The
small-bucket noise problem is a display thing — I handle it with a `total_loans > N`
filter in the query, not by destroying detail in the table.

And it paid off in the real data: inside "Accommodation & Food Services," the true
default rate ran from **2.8% for B&B inns up to 9.1% for snack bars** — a 3x spread
*inside a single sector*. If I'd rolled up to 2-digit, I would've buried exactly the
distinction an underwriter would want to see.

One note: since the data spans 35+ years, it crosses multiple NAICS revisions, so
old codes (`7221xx`) and current ones (`7225xx`) both show up. That's expected
historical drift, not a join problem — I documented it so nobody panics.

### 5.5 `fct_loan_performance_by_geography`

One row per (state, fiscal year), ~2K rows. I used `project_state` (where the funded
business actually is) over `borrower_state`, and derived a `census_region` column so
I can do regional rollups and set up a future join to the Census business-formation
data.

For territories — Puerto Rico, Guam, the Virgin Islands, and a few freely-associated
states like Palau and Micronesia — I bucketed them as `'US Territories'`. The Census
region system doesn't assign these to a region anyway, so this keeps things
standards-aligned, doesn't throw away data, and keeps a future Census join clean
(Census BFS only covers the 50 states + DC). The label's a slight simplification
since the bucket mixes true territories with freely-associated states, but I
documented that honestly.

The data tells a real story: the big economies lead on volume (CA, TX, NY, OH, FL),
and the Sun Belt 2010–2015 vintages (TX, FL, GA around 7%) run hotter than the
coastal and upper-Midwest states — exactly the housing-crash hangover you'd expect.

---

## 6. How I think about testing

I've got 201 dbt tests across 15 models. Every model checks its grain (uniqueness on
the key), not-null on the required fields, accepted values on the enum columns, and
range checks on the rates and years. Two habits:

- I use `severity: warn` for data-quirk checks (the negative amount, year bounds) —
  stuff I want to *see* without it *breaking* the pipeline.
- Tests catch what I miss. Two real examples: a `'7(A)'`-vs-`'7A'` typo in an enum
  test caught in about 9 seconds at the intermediate layer (it would've silently
  returned zero rows in every downstream query), and the too-aggressive quarantine
  filter from §3.3, caught by a row-count comparison.

---

## 7. What's left to do

| Thing | Status | Notes |
|---|---|---|
| Dagster orchestration | **Not started** | My extractors still run by hand. This is the biggest piece left — turning manual one-at-a-time runs into one orchestrated graph. Probably the strongest thing I can add for a data-engineering role. |
| Unified BLS+FRED model | **On hold** | Building it when Mart 4 tells me what cadence it needs (§4.5). |
| Mart 4 — loans + macro context | **Not started** | The ambitious one — joins loans to FRED/BLS at approval date and finally puts the economic data to use. Forces me to build the unified model above. |
| Census business-formation marts | **Not started** | The `census_region` column in the geography mart is the bridge to these. |
| `sqlfluff` formatting pass | **Saving for the end** | One cleanup commit across the whole repo once the marts are done. |
| Pull history / change tracking | **Left out on purpose** | dbt Snapshots is the path if I ever need it (§2.1). |

---

## 8. Conventions, for reference

- **Models:** snake_case. Each `.sql` has a matching `.yml` next to it
  (`stg_bls_series_raw.sql` + `stg_bls_series_raw.yml`).
- **Materialization:** views for staging and intermediate (cheap, always fresh),
  tables for marts (queried a lot, worth precomputing).
- **Schemas:** `models/staging → SBLI.STAGING`, `intermediate → SBLI.INTERMEDIATE`,
  `marts → SBLI.MARTS`, wired up in `dbt_project.yml` plus a custom
  `generate_schema_name.sql` macro that strips dbt's default schema prefixing.
- **Sources:** declared in `models/staging/sources.yml` under the short name `raw`.
- **Secrets:** `.env`, `~/.dbt/profiles.yml`, and the key-pair `.p8` file all live
  outside the repo / are gitignored. Snowflake auth is key-pair (JWT), not a password.