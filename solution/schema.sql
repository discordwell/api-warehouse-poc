-- API → Warehouse POC: DuckDB Schema
-- Run this to initialize the warehouse tables

-- ============================================
-- FRED (Federal Reserve Economic Data)
-- ============================================

-- Series metadata
CREATE TABLE IF NOT EXISTS fred_series (
    series_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    observation_start DATE,
    observation_end DATE,
    frequency VARCHAR,
    frequency_short VARCHAR(10),
    units VARCHAR,
    units_short VARCHAR(50),
    seasonal_adjustment VARCHAR,
    seasonal_adjustment_short VARCHAR(10),
    last_updated TIMESTAMP,
    popularity INTEGER,
    notes TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Time series observations
CREATE TABLE IF NOT EXISTS fred_observations (
    series_id VARCHAR NOT NULL,
    observation_date DATE NOT NULL,
    value DOUBLE,
    realtime_start DATE,
    realtime_end DATE,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (series_id, observation_date),
    FOREIGN KEY (series_id) REFERENCES fred_series(series_id)
);

-- Index for faster time-based queries
CREATE INDEX IF NOT EXISTS idx_fred_obs_date ON fred_observations(observation_date);


-- ============================================
-- Hacker News
-- ============================================

-- Items (stories, comments, jobs, polls, pollopts)
CREATE TABLE IF NOT EXISTS hn_items (
    id INTEGER PRIMARY KEY,
    type VARCHAR(20),
    "by" VARCHAR(100),  -- Quoted: 'by' is a reserved word
    time INTEGER,  -- Unix timestamp
    text TEXT,
    parent INTEGER,
    url VARCHAR(2000),
    score INTEGER,
    title VARCHAR(500),
    descendants INTEGER,  -- Total comment count (for stories)
    dead BOOLEAN DEFAULT FALSE,
    deleted BOOLEAN DEFAULT FALSE,
    poll INTEGER,  -- For pollopts
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Separate kids relationship table for normalized storage
CREATE TABLE IF NOT EXISTS hn_item_kids (
    item_id INTEGER NOT NULL,
    kid_id INTEGER NOT NULL,
    position INTEGER,  -- Order in the kids array
    PRIMARY KEY (item_id, kid_id),
    FOREIGN KEY (item_id) REFERENCES hn_items(id)
);

-- Users
CREATE TABLE IF NOT EXISTS hn_users (
    id VARCHAR(100) PRIMARY KEY,
    created INTEGER,  -- Unix timestamp
    karma INTEGER,
    about TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_hn_items_type ON hn_items(type);
CREATE INDEX IF NOT EXISTS idx_hn_items_parent ON hn_items(parent);
CREATE INDEX IF NOT EXISTS idx_hn_items_by ON hn_items("by");
CREATE INDEX IF NOT EXISTS idx_hn_items_time ON hn_items(time);


-- ============================================
-- Sync Metadata (for incremental loads)
-- ============================================

CREATE TABLE IF NOT EXISTS sync_metadata (
    source VARCHAR(50) PRIMARY KEY,
    last_sync_at TIMESTAMP,
    last_sync_status VARCHAR(20),
    records_synced INTEGER,
    error_message TEXT
);


-- ============================================
-- Schema Documentation
-- ============================================

/*
FRED Tables:
- fred_series: Metadata about each economic indicator series
- fred_observations: Actual time series data points

Hacker News Tables:
- hn_items: All items (stories, comments, etc.) in denormalized form
- hn_item_kids: Normalized child relationships (avoids array storage)
- hn_users: User profiles for authors

Design Decisions:
1. Used composite primary key for fred_observations to prevent duplicates
2. Stored HN timestamps as integers (Unix) to match API format
3. Created hn_item_kids to normalize the kids array for better querying
4. Added ingested_at timestamps for audit trail
5. sync_metadata enables idempotent incremental loads

Query Patterns Supported:
- Time series analysis on FRED data
- Story/comment threading on HN data
- Author activity analysis
- Cross-source analysis (economic news correlation)
*/
