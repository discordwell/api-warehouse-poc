# Data Engineering Challenge: Build an API → Warehouse Pipeline

## Your Mission

You are a data engineer. Your task is to build a local data warehouse that ingests, normalizes, and stores data from two public APIs.

**No hand-holding.** You have sample API responses and documentation links. Figure out the rest.

---

## APIs to Ingest

### 1. FRED (Federal Reserve Economic Data)

Federal Reserve's economic data API. Contains time series for economic indicators.

- **Base URL**: `https://api.stlouisfed.org/fred/`
- **Documentation**: https://fred.stlouisfed.org/docs/api/fred/
- **Authentication**: Free API key required
  - Get one instantly at: https://fred.stlouisfed.org/docs/api/api_key.html

**Data to ingest:**
| Series ID | What it is |
|-----------|------------|
| GDP | Gross Domestic Product (quarterly) |
| UNRATE | Unemployment Rate (monthly) |
| CPIAUCSL | Consumer Price Index (monthly) |
| FEDFUNDS | Federal Funds Rate (monthly) |

See `fred_sample.json` for example API responses.

---

### 2. Hacker News (Firebase API)

Tech news aggregator. Stories have nested comment threads.

- **Base URL**: `https://hacker-news.firebaseio.com/v0/`
- **Documentation**: https://github.com/HackerNews/API
- **Authentication**: None required

**Data to ingest:**
- Top 100 stories (from `/topstories.json`)
- All comments for each story (recursive - comments have child comments)
- User profiles for story authors

See `hn_sample.json` for example API responses.

---

## Requirements

### Must Have
1. **DuckDB database** (`warehouse.duckdb`) with proper relational schema
2. **Incremental loading** - running twice shouldn't duplicate data
3. **Error handling** - retries, graceful failures, logging
4. **Normalized tables** - flatten nested JSON into proper relations

### Deliverables
```
solution/
├── schema.sql           # Table definitions
├── ingest_fred.py       # FRED ingestion script
├── ingest_hn.py         # Hacker News ingestion script
├── warehouse.duckdb     # The populated database
└── requirements.txt     # Python dependencies
```

---

## Constraints

- Python 3.10+
- DuckDB for storage (not SQLite, Postgres, etc.)
- No ORM - write raw SQL for schema
- Must handle API rate limits gracefully

---

## Verification

After you build it, these queries should work:

```sql
-- Should return 4 series
SELECT series_id, title FROM fred_series;

-- Should return latest unemployment rate
SELECT observation_date, value
FROM fred_observations
WHERE series_id = 'UNRATE'
ORDER BY observation_date DESC
LIMIT 1;

-- Should return ~100 stories
SELECT COUNT(*) FROM hn_items WHERE type = 'story';

-- Should show story-comment relationships
SELECT title, descendants as comment_count
FROM hn_items
WHERE type = 'story'
ORDER BY descendants DESC
LIMIT 5;
```

---

## Hints

Don't read these unless you're stuck.

<details>
<summary>Hint 1: FRED API structure</summary>

FRED has two key endpoints:
- `/series` - metadata about a series
- `/series/observations` - actual data points

You'll need to call both for each series.
</details>

<details>
<summary>Hint 2: HN comment threading</summary>

Comments are recursive. A story has `kids` (direct comments), and each comment can have its own `kids` (replies).

Use BFS or DFS to fetch the full tree. Watch out for deleted items.
</details>

<details>
<summary>Hint 3: Incremental loads</summary>

For FRED: track the last observation date per series, only fetch newer data.

For HN: check if item ID already exists before inserting.
</details>

---

## Go

Build it. The sample data and docs are all you get.

Time starts now.
