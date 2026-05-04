-- ============================================================================
-- DOMAIN DEDUPLICATION SCRIPT
-- ============================================================================
-- Purpose: Deduplicates company records by normalized website domain name.
--          Keeps only companies whose website domain is unique across the
--          entire dataset, plus companies with no URL at all.
--
-- Tables processed:
--   1. definitive_healthcare.`ALF Overview`
--   2. definitive_healthcare.`SNF Overview`
--
-- Domain Normalization Steps:
--   1. Trim whitespace
--   2. Strip http:// or https:// protocol
--   3. Strip leading www.
--   4. Strip everything after the first / (path, query string, etc.)
--   5. Lowercase everything
--
-- Example: https://www.brookdale.com/en/communities/brookdale-sumter.html
--       → brookdale.com
--
-- Filter Logic:
--   - Count how many companies share each normalized domain
--   - KEEP  if domain appears exactly once (unique)
--   - KEEP  if there is no URL (blank/null)
--   - REMOVE if domain appears 2+ times anywhere in the table
--
-- Results Summary (as of run date):
--   ALF Overview: 46,185 total → 18,657 removed → 27,528 kept (19,122 no URL, 8,406 unique URL)
--   SNF Overview: 21,232 total →  9,246 removed → 11,986 kept ( 1,469 no URL, 10,517 unique URL)
-- ============================================================================

-- ============================================================================
-- PART 1: ALF Overview — Deduplicated Results
-- ============================================================================

CREATE OR REPLACE TABLE `gen-lang-client-0844868008.definitive_healthcare.ALF Overview Deduplicated` AS
WITH normalized AS (
  SELECT
    *,
    -- Normalize the WEBSITE column to a root domain
    REGEXP_REPLACE(
      LOWER(REGEXP_REPLACE(TRIM(WEBSITE), r'^https?://(www\.)?', '')),
      r'/.*$', ''
    ) AS domain
  FROM `gen-lang-client-0844868008.definitive_healthcare.ALF Overview`
),
domain_counts AS (
  SELECT
    domain,
    COUNT(*) AS cnt
  FROM normalized
  WHERE domain IS NOT NULL AND domain != ''
  GROUP BY domain
)
SELECT
  n.* EXCEPT(domain)  -- exclude the helper column from output
FROM normalized n
LEFT JOIN domain_counts dc ON n.domain = dc.domain
WHERE dc.cnt = 1 OR dc.domain IS NULL OR dc.domain = '';


-- ============================================================================
-- PART 2: SNF Overview — Deduplicated Results
-- ============================================================================

CREATE OR REPLACE TABLE `gen-lang-client-0844868008.definitive_healthcare.SNF Overview Deduplicated` AS
WITH normalized AS (
  SELECT
    *,
    -- Normalize the WEBSITE column to a root domain
    REGEXP_REPLACE(
      LOWER(REGEXP_REPLACE(TRIM(WEBSITE), r'^https?://(www\.)?', '')),
      r'/.*$', ''
    ) AS domain
  FROM `gen-lang-client-0844868008.definitive_healthcare.SNF Overview`
),
domain_counts AS (
  SELECT
    domain,
    COUNT(*) AS cnt
  FROM normalized
  WHERE domain IS NOT NULL AND domain != ''
  GROUP BY domain
)
SELECT
  n.* EXCEPT(domain)  -- exclude the helper column from output
FROM normalized n
LEFT JOIN domain_counts dc ON n.domain = dc.domain
WHERE dc.cnt = 1 OR dc.domain IS NULL OR dc.domain = '';


-- ============================================================================
-- PART 3: Validation — Summary Statistics for Both Tables
-- ============================================================================

-- ALF Overview Summary
SELECT
  'ALF Overview' AS table_name,
  COUNT(*) AS total_input,
  SUM(CASE WHEN dc.cnt >= 2 THEN 1 ELSE 0 END) AS removed_shared_domain,
  SUM(CASE WHEN dc.cnt = 1 OR dc.domain IS NULL OR dc.domain = '' THEN 1 ELSE 0 END) AS kept,
  SUM(CASE WHEN dc.domain IS NULL OR dc.domain = '' THEN 1 ELSE 0 END) AS kept_no_url,
  SUM(CASE WHEN dc.cnt = 1 THEN 1 ELSE 0 END) AS kept_unique_url
FROM (
  SELECT
    REGEXP_REPLACE(
      LOWER(REGEXP_REPLACE(TRIM(WEBSITE), r'^https?://(www\.)?', '')),
      r'/.*$', ''
    ) AS domain
  FROM `gen-lang-client-0844868008.definitive_healthcare.ALF Overview`
) n
LEFT JOIN (
  SELECT domain, COUNT(*) AS cnt
  FROM (
    SELECT
      REGEXP_REPLACE(
        LOWER(REGEXP_REPLACE(TRIM(WEBSITE), r'^https?://(www\.)?', '')),
        r'/.*$', ''
      ) AS domain
    FROM `gen-lang-client-0844868008.definitive_healthcare.ALF Overview`
  )
  WHERE domain IS NOT NULL AND domain != ''
  GROUP BY domain
) dc ON n.domain = dc.domain

UNION ALL

-- SNF Overview Summary
SELECT
  'SNF Overview' AS table_name,
  COUNT(*) AS total_input,
  SUM(CASE WHEN dc.cnt >= 2 THEN 1 ELSE 0 END) AS removed_shared_domain,
  SUM(CASE WHEN dc.cnt = 1 OR dc.domain IS NULL OR dc.domain = '' THEN 1 ELSE 0 END) AS kept,
  SUM(CASE WHEN dc.domain IS NULL OR dc.domain = '' THEN 1 ELSE 0 END) AS kept_no_url,
  SUM(CASE WHEN dc.cnt = 1 THEN 1 ELSE 0 END) AS kept_unique_url
FROM (
  SELECT
    REGEXP_REPLACE(
      LOWER(REGEXP_REPLACE(TRIM(WEBSITE), r'^https?://(www\.)?', '')),
      r'/.*$', ''
    ) AS domain
  FROM `gen-lang-client-0844868008.definitive_healthcare.SNF Overview`
) n
LEFT JOIN (
  SELECT domain, COUNT(*) AS cnt
  FROM (
    SELECT
      REGEXP_REPLACE(
        LOWER(REGEXP_REPLACE(TRIM(WEBSITE), r'^https?://(www\.)?', '')),
        r'/.*$', ''
      ) AS domain
    FROM `gen-lang-client-0844868008.definitive_healthcare.SNF Overview`
  )
  WHERE domain IS NOT NULL AND domain != ''
  GROUP BY domain
) dc ON n.domain = dc.domain;


-- ============================================================================
-- PART 4 (Optional): Query the deduplicated tables directly
-- ============================================================================

-- SELECT * FROM `gen-lang-client-0844868008.definitive_healthcare.ALF Overview Deduplicated`;
-- SELECT * FROM `gen-lang-client-0844868008.definitive_healthcare.SNF Overview Deduplicated`;