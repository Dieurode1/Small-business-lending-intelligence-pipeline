-- ============================================================================
-- CASE STUDY: Pioneer Valley Bank & Trust
-- Using the SBLI pipeline to answer a Chief Credit Officer's questions
-- about SBA portfolio risk.
--
-- Pioneer Valley Bank & Trust is a FICTIONAL institution created to illustrate
-- how the Small Business Lending Intelligence (SBLI) pipeline answers real
-- credit-risk questions. The bank is invented; the data, queries, and findings
-- are real — every query below runs against the pipeline's marts.
--
-- THE CLIENT:
--   ~$2B-asset community bank, Connecticut River Valley (Western MA).
--   ~$180M SBA 7(a)/504 book, grown fast over 5 years.
--   New Chief Credit Officer, Marcus Reyes, hired late 2025.
--   Board's question: "Is our SBA portfolio a hidden risk — and how would
--   we even know?" He has his own loan tape but no industry context to
--   benchmark against. The SBLI pipeline provides that context.
-- ============================================================================

USE ROLE SBLI_ROLE;
USE WAREHOUSE SBLI_WH;
USE DATABASE SBLI;
USE SCHEMA MARTS;

-- ============================================================================
-- QUESTION 1 — "Are we lending like it's 2006 right now?"
-- The vintage question: does underwriting-era credit discipline predict
-- how a loan cohort defaults, independent of the borrower?
--
-- Real finding (this query): the 2007 vintage defaults at 31.7% vs just
-- 4.9% for 2012 — a 6.5x gap. Every boom vintage (2004-2007) was underwritten
-- while lending standards (SLOOS net-tightening) were NEGATIVE, i.e. banks
-- loosening. Standards flipped sharply positive (tightening) through the 2008
-- crash, and the vintages written into that discipline (2010-2013) default
-- around 5%. Lesson: underwriting discipline AT ORIGINATION outweighs the
-- macro climate the loan later lives through.
-- ============================================================================

SELECT
    approval_year,
    SUM(total_loans)                                    AS loans_originated,
    ROUND(AVG(true_default_rate) * 100, 1)              AS avg_true_default_rate_pct,
    ROUND(AVG(lending_standards), 1)                    AS avg_lending_standards,
    -- fred_recession_probability is already a 0-100 percentage; no *100
    ROUND(AVG(fred_recession_probability), 1)           AS avg_recession_prob_pct
FROM fct_loans_with_macro_context
WHERE approval_year BETWEEN 2004 AND 2013
GROUP BY approval_year
ORDER BY approval_year;

-- ============================================================================
-- QUESTION 2 — "Which industries should we lean into or avoid?"
-- Concentration risk by 6-digit NAICS. Default rates vary enormously by
-- sub-industry, and the spread persists even WITHIN a broad sector — a
-- sector-level average would hide it. (e.g. within Food Services, B&B inns
-- ~2.8% vs snack/beverage bars ~9.1%.) Lets the bank set concentration
-- limits at the sub-industry level, not the blunt sector level.
-- ============================================================================

SELECT
    naics_code,
    naics_description,
    SUM(total_loans)                                    AS loans,
    TO_VARCHAR(SUM(total_charge_off_dollars), '999,999,999,999') AS charge_off_dollars,
    ROUND(
        SUM(total_charged_off + total_sba_purchased)
        / NULLIF(SUM(total_loans), 0) * 100, 1)         AS true_default_rate_pct
FROM fct_loan_performance_by_industry
GROUP BY naics_code, naics_description
HAVING SUM(total_loans) >= 500          -- ignore thin, noisy sectors
ORDER BY true_default_rate_pct DESC
LIMIT 20;

-- ============================================================================
-- QUESTION 3 — "How does our geographic footprint expose us?"
-- Default risk by state. Gives Marcus a risk-adjusted map before expanding
-- outside the New England footprint: which states to price for / avoid,
-- which are comparatively safe for measured expansion.
-- ============================================================================

SELECT
    project_state,
    SUM(total_loans)                                    AS loans,
    TO_VARCHAR(SUM(total_approved_dollars), '999,999,999,999') AS approved_dollars,
    ROUND(
        SUM(total_charged_off + total_sba_purchased)
        / NULLIF(SUM(total_loans), 0) * 100, 1)         AS true_default_rate_pct
FROM fct_loan_performance_by_geography
GROUP BY project_state
HAVING SUM(total_loans) >= 1000
ORDER BY true_default_rate_pct DESC;

-- ============================================================================
-- QUESTION 4 — "Who are the best-performing lenders, and what can we learn?"
-- Benchmarking against the field: high-volume, low-default originators are
-- a template for disciplined SBA lending and a source of realistic targets.
-- ============================================================================

SELECT
    lender_name,
    SUM(total_loans)                                    AS loans,
    TO_VARCHAR(SUM(total_approved_dollars), '999,999,999,999') AS approved_dollars,
    ROUND(
        SUM(total_charged_off + total_sba_purchased)
        / NULLIF(SUM(total_loans), 0) * 100, 1)         AS true_default_rate_pct
FROM fct_lender_performance
GROUP BY lender_name
HAVING SUM(total_loans) >= 2000        -- meaningful volume only
ORDER BY true_default_rate_pct ASC
LIMIT 25;

-- ============================================================================
-- QUESTION 5 — "Is the macro environment flashing a warning right now?"
-- The leading indicator. Q1 showed lending standards at origination predict
-- vintage performance — so compare where standards sit in the most recent
-- years vs the 2004-2007 danger-zone signature (negative/loosening standards
-- alongside a rising rate environment). Turns a backward-looking loan tape
-- into a forward-looking risk signal.
-- ============================================================================

SELECT
    approval_year,
    ROUND(AVG(lending_standards), 1)                    AS avg_lending_standards,
    ROUND(AVG(fred_fed_funds_rate), 2)                  AS avg_fed_funds,
    ROUND(AVG(fred_unemployment_rate), 1)               AS avg_unemployment,
    -- already a 0-100 percentage; no *100
    ROUND(AVG(fred_recession_probability), 1)           AS avg_recession_prob_pct
FROM fct_loans_with_macro_context
WHERE approval_year >= 2000
GROUP BY approval_year
ORDER BY approval_year;

-- ============================================================================
-- END OF CASE STUDY WORKSHEET
-- The differentiator across all five questions is the macro-contextualized
-- vintage view (fct_loans_with_macro_context): joining each loan cohort to
-- the economic environment at origination is what turns "here are our
-- defaults" into "here's WHY, and here's what to watch."
-- ============================================================================
