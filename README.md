# API → Data Warehouse POC

## Challenge

You have access to two public APIs. Your task is to build a local data warehouse that ingests, normalizes, and stores data from both sources.

## APIs

### 1. FRED (Federal Reserve Economic Data)
- **Base URL**: `https://api.stlouisfed.org/fred/`
- **Auth**: API key (free, instant registration at https://fred.stlouisfed.org/docs/api/api_key.html)
- **Documentation**: https://fred.stlouisfed.org/docs/api/fred/

**Required series to ingest**:
| Series ID | Description |
|-----------|-------------|
| GDP | Gross Domestic Product |
| UNRATE | Unemployment Rate |
| CPIAUCSL | Consumer Price Index |
| FEDFUNDS | Federal Funds Rate |

### 2. Hacker News (Firebase API)
- **Base URL**: `https://hacker-news.firebaseio.com/v0/`
- **Auth**: None required
- **Documentation**: https://github.com/HackerNews/API

**Required data to ingest**:
- Top 100 stories (from `/topstories.json`)
- All comments for each story (recursive fetch)
- User profiles for story authors

## Requirements

### Functional
1. Store all data in a local DuckDB database (`warehouse.duckdb`)
2. Handle incremental updates (re-running should not duplicate data)
3. Normalize nested JSON structures into proper relational tables
4. Handle API errors gracefully (retries, rate limits, timeouts)

### Technical
1. Python 3.10+
2. DuckDB for storage
3. Proper error handling and logging
4. Type hints encouraged

### Deliverables
1. `ingest_fred.py` - FRED data ingestion script
2. `ingest_hn.py` - Hacker News ingestion script
3. `schema.sql` - DDL for all tables
4. `warehouse.duckdb` - Populated database
5. Brief documentation of your schema design

## Sample API Responses

See `challenge/` directory for sample JSON responses from each API.

## Verification

After building the warehouse, these queries should work (see `verify/test_queries.sql`):

```sql
-- FRED: Get latest unemployment rate
SELECT observation_date, value
FROM fred_observations
WHERE series_id = 'UNRATE'
ORDER BY observation_date DESC
LIMIT 1;

-- HN: Count stories by type
SELECT type, COUNT(*)
FROM hn_items
GROUP BY type;

-- HN: Find most commented stories
SELECT title, descendants as comment_count
FROM hn_items
WHERE type = 'story'
ORDER BY descendants DESC
LIMIT 10;
```

## Success Criteria

- [ ] DuckDB warehouse created with normalized schema
- [ ] All 4 FRED series ingested with historical data
- [ ] Top 100 HN stories ingested with comments
- [ ] Incremental load works (re-run doesn't duplicate)
- [ ] Verification queries return expected results
- [ ] Code handles API errors gracefully
