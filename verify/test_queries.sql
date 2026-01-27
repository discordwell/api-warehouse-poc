-- Verification Queries for API → Warehouse POC
-- Run these against warehouse.duckdb to validate the implementation

-- ============================================
-- FRED Data Verification
-- ============================================

-- 1. Check all required series were ingested
SELECT series_id, title, frequency, units, COUNT(*) as observation_count
FROM fred_series s
JOIN fred_observations o ON s.series_id = o.series_id
WHERE s.series_id IN ('GDP', 'UNRATE', 'CPIAUCSL', 'FEDFUNDS')
GROUP BY s.series_id, s.title, s.frequency, s.units
ORDER BY s.series_id;
-- EXPECTED: 4 rows, one for each series

-- 2. Get the latest observation for each series
SELECT
    s.series_id,
    s.title,
    o.observation_date,
    o.value,
    s.units
FROM fred_series s
JOIN fred_observations o ON s.series_id = o.series_id
WHERE (s.series_id, o.observation_date) IN (
    SELECT series_id, MAX(observation_date)
    FROM fred_observations
    GROUP BY series_id
)
ORDER BY s.series_id;
-- EXPECTED: 4 rows with recent dates

-- 3. Check for duplicate observations (should be 0)
SELECT series_id, observation_date, COUNT(*) as cnt
FROM fred_observations
GROUP BY series_id, observation_date
HAVING COUNT(*) > 1;
-- EXPECTED: 0 rows (no duplicates)

-- 4. Verify date range coverage (at least 10 years of data)
SELECT
    series_id,
    MIN(observation_date) as earliest,
    MAX(observation_date) as latest,
    DATE_DIFF('year', MIN(observation_date), MAX(observation_date)) as years_of_data
FROM fred_observations
GROUP BY series_id;
-- EXPECTED: At least 10 years of data for each series


-- ============================================
-- Hacker News Data Verification
-- ============================================

-- 5. Check item types ingested
SELECT type, COUNT(*) as count
FROM hn_items
GROUP BY type
ORDER BY count DESC;
-- EXPECTED: 'story' and 'comment' types present

-- 6. Count stories (should be ~100)
SELECT COUNT(*) as story_count
FROM hn_items
WHERE type = 'story';
-- EXPECTED: ~100 stories

-- 7. Find top 10 most commented stories
SELECT
    id,
    title,
    score,
    descendants as comment_count,
    "by" as author,
    to_timestamp(time) as posted_at
FROM hn_items
WHERE type = 'story'
ORDER BY descendants DESC NULLS LAST
LIMIT 10;
-- EXPECTED: 10 stories with comment counts

-- 8. Verify comment-story relationships
SELECT
    s.id as story_id,
    s.title,
    COUNT(c.id) as actual_comments,
    s.descendants as reported_comments
FROM hn_items s
LEFT JOIN hn_items c ON c.parent = s.id OR c.parent IN (
    SELECT id FROM hn_items WHERE parent = s.id
)
WHERE s.type = 'story'
GROUP BY s.id, s.title, s.descendants
HAVING COUNT(c.id) > 0
ORDER BY actual_comments DESC
LIMIT 10;
-- EXPECTED: Stories with their comments

-- 9. Check for orphaned comments (comments without valid parent)
SELECT COUNT(*) as orphan_count
FROM hn_items c
WHERE c.type = 'comment'
AND c.parent NOT IN (SELECT id FROM hn_items);
-- EXPECTED: 0 or very few orphans (some may be from deleted items)

-- 10. Verify users were captured
SELECT COUNT(DISTINCT "by") as unique_authors
FROM hn_items
WHERE "by" IS NOT NULL;
-- EXPECTED: Multiple unique authors

-- 11. Check user data (if users table exists)
-- SELECT COUNT(*) FROM hn_users;
-- EXPECTED: User records for story authors


-- ============================================
-- Data Quality Checks
-- ============================================

-- 12. Check for null values in critical fields (FRED)
SELECT
    'fred_observations' as table_name,
    SUM(CASE WHEN series_id IS NULL THEN 1 ELSE 0 END) as null_series_id,
    SUM(CASE WHEN observation_date IS NULL THEN 1 ELSE 0 END) as null_date,
    SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) as null_value
FROM fred_observations;
-- EXPECTED: null_series_id and null_date should be 0

-- 13. Check for null values in critical fields (HN)
SELECT
    'hn_items' as table_name,
    SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) as null_id,
    SUM(CASE WHEN type IS NULL THEN 1 ELSE 0 END) as null_type,
    SUM(CASE WHEN time IS NULL THEN 1 ELSE 0 END) as null_time
FROM hn_items;
-- EXPECTED: All should be 0


-- ============================================
-- Incremental Load Verification
-- ============================================

-- 14. Check for metadata tracking (if implemented)
-- This verifies that the solution tracks sync state for incremental loads
-- SELECT * FROM sync_metadata;
-- EXPECTED: Timestamps for last successful sync per source


-- ============================================
-- Cross-Source Analysis (Bonus)
-- ============================================

-- 15. Stories about economic topics (join concept verification)
SELECT
    h.title,
    h.score,
    datetime(h.time, 'unixepoch') as posted_at
FROM hn_items h
WHERE h.type = 'story'
AND (
    LOWER(h.title) LIKE '%gdp%'
    OR LOWER(h.title) LIKE '%inflation%'
    OR LOWER(h.title) LIKE '%unemployment%'
    OR LOWER(h.title) LIKE '%federal reserve%'
    OR LOWER(h.title) LIKE '%economy%'
)
ORDER BY h.score DESC
LIMIT 5;
-- EXPECTED: May return 0-5 rows depending on current top stories
