# Case Study: Pioneer Valley Bank & Trust
### Using the SBLI pipeline to answer a Chief Credit Officer's questions about SBA portfolio risk

> **Note:** Pioneer Valley Bank & Trust is a fictional institution created to illustrate how the Small Business Lending Intelligence (SBLI) pipeline answers real credit-risk questions. The bank is invented; the data, queries, and findings are real — every query below runs against the pipeline's marts and returns an actual result.

---

## The client

**Pioneer Valley Bank & Trust** is a ~$2B-asset community bank founded in 1987, headquartered in the Connecticut River Valley of Western Massachusetts. It runs a relationship-driven SBA lending desk — roughly a $180M book across SBA 7(a) and 504 programs — that has grown quickly over the last five years.

In late 2025, after the regional-banking stress of 2023–24, the board hired a new **Chief Credit Officer, Marcus Reyes**. In his first audit-committee meeting, the board asked him one blunt question:

> *"Is our SBA portfolio a hidden risk — and how would we even know?"*

Marcus has his own bank's loan tape. What he lacks is **context**: 30+ years of industry-wide SBA lending outcomes to benchmark against. Is the bank's current underwriting aggressive or conservative relative to history? Which industries and geographies actually default? What does the macro environment say about loans being written *right now*?

That context is exactly what the SBLI pipeline provides — the full SBA 7(a) and 504 FOIA population (~2.17M loans), enriched with the macroeconomic environment at each loan's origination. Below are the five questions Marcus brought to his analytics team, and the queries that answered them.

---

## Question 1 — "Are we lending like it's 2006 right now?"
### The vintage question — the one that keeps Marcus up at night

**Business context.** Marcus's core fear is *timing risk*: that loans underwritten in a loose-credit boom default at far higher rates than the same loans written in a disciplined environment — regardless of the borrower. If the industry's worst-performing vintages line up with loose lending standards, that's a template for what to watch today.

**Query** (against `fct_loans_with_macro_context`):

```sql
SELECT
    approval_year,
    SUM(total_loans)                                    AS loans_originated,
    ROUND(AVG(true_default_rate) * 100, 1)              AS avg_true_default_rate_pct,
    ROUND(AVG(lending_standards), 1)                    AS avg_lending_standards
FROM SBLI.MARTS.fct_loans_with_macro_context
WHERE approval_year BETWEEN 2004 AND 2013
GROUP BY approval_year
ORDER BY approval_year;
```

**What it shows.** The result is stark. The **2007 vintage defaults at 31.7%; the 2012 vintage at just 4.9% — a 6.5x gap.** And the pattern is systematic: every boom vintage (2004–2007) was underwritten while `lending_standards` (the Fed's SLOOS net-tightening measure) sat *negative* — banks were loosening. Standards then flipped sharply positive (tightening) through the 2008 crash, and the vintages written into that discipline (2010–2013) default around 5%.

| Vintage year | True default rate | Lending standards |
|---|---|---|
| 2005 | 22.7% | -18.3 (loose) |
| 2006 | 29.0% | -8.0 (loose) |
| 2007 | 31.7% | +5.7 |
| 2008 | 21.1% | +57.2 (very tight) |
| 2011 | 5.6% | -13.7 |
| 2012 | 4.9% | -4.6 |

**So what for Pioneer Valley.** The lesson isn't "avoid recessions" — loans *written during* the scary period actually did fine. The lesson is **underwriting discipline at origination outweighs the macro climate the loan lives through.** Marcus's actionable takeaway: watch where lending standards sit *today* (Question 5), because that's the leading indicator of how today's originations will age.

---

## Question 2 — "Which industries should we lean into or avoid?"
### Concentration risk by sector

**Business context.** Pioneer Valley's SBA desk has been growing by saying yes to whatever deals cross the desk. Marcus wants to know whether some industries carry structurally higher default risk — so the bank can set concentration limits rather than discover the risk after the fact.

**Query** (against `fct_loan_performance_by_industry`):

```sql
SELECT
    naics_code,
    naics_description,
    SUM(total_loans)                                        AS loans,
    ROUND(SUM(total_charge_off_dollars) / 1e6, 1)          AS charge_off_dollars_mm,
    ROUND(
        SUM(total_charged_off + total_sba_purchased)
        / NULLIF(SUM(total_loans), 0) * 100, 1)            AS true_default_rate_pct
FROM SBLI.MARTS.fct_loan_performance_by_industry
GROUP BY naics_code, naics_description
HAVING SUM(total_loans) >= 500          -- ignore thin, noisy sectors
ORDER BY true_default_rate_pct DESC
LIMIT 20;
```

**What it shows.** Default rates vary enormously by 6-digit NAICS industry — and the spread persists even *within* a single broad sector. (In Accommodation & Food Services, for example, the pipeline surfaces a range from ~2.8% for bed-and-breakfast inns to ~9.1% for snack/beverage bars — a 3x difference that a sector-level average would completely hide.)

**So what for Pioneer Valley.** Marcus can set differentiated concentration limits and pricing at the *sub-industry* level, not the broad-sector level. A blanket "restaurants are risky" policy would reject good B&B deals and over-concentrate in the wrong sub-sectors. The 6-digit grain lets the bank be precise.

---

## Question 3 — "How does our geographic footprint expose us?"
### Should we lend outside New England?

**Business context.** Pioneer Valley is being asked to fund deals outside its Connecticut River Valley footprint. Before expanding, Marcus wants to know whether default risk varies meaningfully by state and region — and whether the bank's home turf is actually a safe base.

**Query** (against `fct_loan_performance_by_geography`):

```sql
SELECT
    project_state,
    SUM(total_loans)                                       AS loans,
    ROUND(SUM(total_approved_dollars) / 1e9, 2)           AS approved_dollars_bn,
    ROUND(
        SUM(total_charged_off + total_sba_purchased)
        / NULLIF(SUM(total_loans), 0) * 100, 1)           AS true_default_rate_pct
FROM SBLI.MARTS.fct_loan_performance_by_geography
GROUP BY project_state
HAVING SUM(total_loans) >= 1000
ORDER BY true_default_rate_pct DESC;
```

**So what for Pioneer Valley.** The ranking gives Marcus a risk-adjusted map: which states carry elevated default rates (to price for or avoid) and which are comparatively safe (candidates for measured expansion). He can benchmark his own New England footprint against the national field before committing capital to unfamiliar geography.

---

## Question 4 — "Who are the best-performing lenders, and what can we learn?"
### Benchmarking against the field

**Business context.** Marcus wants to know how the best SBA originators in the country perform — both as a benchmark for Pioneer Valley's own numbers and to identify what disciplined origination looks like at scale.

**Query** (against `fct_lender_performance`):

```sql
SELECT
    primary_lender_name,
    SUM(total_loans)                                       AS loans,
    ROUND(SUM(total_approved_dollars) / 1e9, 2)           AS approved_dollars_bn,
    ROUND(
        SUM(total_charged_off + total_sba_purchased)
        / NULLIF(SUM(total_loans), 0) * 100, 1)           AS true_default_rate_pct
FROM SBLI.MARTS.fct_lender_performance
GROUP BY primary_lender_name
HAVING SUM(total_loans) >= 2000        -- meaningful volume only
ORDER BY true_default_rate_pct ASC
LIMIT 25;
```

**So what for Pioneer Valley.** The high-volume, low-default lenders are a template. Marcus can study their apparent concentration patterns (cross-referencing Questions 2 and 3) to understand what disciplined SBA origination looks like — and set realistic performance targets for his own desk.

---

## Question 5 — "Is the macro environment flashing a warning right now?"
### The leading indicator

**Business context.** Question 1 established that lending standards at origination predict vintage performance. So the single most important forward-looking question is: **where do lending standards sit today** versus the historical danger zones?

**Query** (against `fct_loans_with_macro_context`, using the macro columns as a time series):

```sql
SELECT
    approval_year,
    ROUND(AVG(lending_standards), 1)                    AS avg_lending_standards,
    ROUND(AVG(fred_fed_funds_rate), 2)                  AS avg_fed_funds,
    ROUND(AVG(fred_unemployment_rate), 1)               AS avg_unemployment
FROM SBLI.MARTS.fct_loans_with_macro_context
WHERE approval_year >= 2000
GROUP BY approval_year
ORDER BY approval_year;
```

**So what for Pioneer Valley.** By reading the most recent rows and comparing them to the 2004–2007 danger-zone signature (persistently negative/loosening standards alongside a rising-rate environment), Marcus gets an early read on whether *today's* originations are being written into a loose-credit environment — the exact pattern that produced the 31.7% class of 2007. It turns a backward-looking loan tape into a forward-looking risk signal.

---

## How the pipeline makes this possible

Every answer above draws on the same architecture:

| Layer | Role in this case study |
|---|---|
| **Extractors → S3 → Snowflake RAW** | Pulls the full SBA 7(a)/504 FOIA population plus FRED & BLS macro series |
| **dbt staging → intermediate** | Cleans, unifies 7(a)+504, and builds the monthly macro spine |
| **Marts** | The four analytics-ready tables every question queries directly |
| **Dagster orchestration** | Runs and monitors the whole flow, with data-quality gates on ingestion |

The differentiator is the **macro-contextualized vintage view** (`fct_loans_with_macro_context`): joining each loan cohort to the economic environment at its origination is what turns "here are our defaults" into "here's *why* they defaulted, and here's what to watch." That's the analysis a community bank like Pioneer Valley can't do from its own loan tape alone — and it's the reason the pipeline earns its keep.
