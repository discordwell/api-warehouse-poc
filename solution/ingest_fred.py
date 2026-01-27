#!/usr/bin/env python3
"""
FRED (Federal Reserve Economic Data) Ingestion Script

Fetches economic time series data from the FRED API and loads it into DuckDB.
Supports incremental updates via observation date tracking.
"""

import os
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import duckdb
import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration
FRED_API_KEY = os.getenv('FRED_API_KEY')
FRED_BASE_URL = 'https://api.stlouisfed.org/fred'
DB_PATH = Path(__file__).parent / 'warehouse.duckdb'

# Series to ingest
SERIES_IDS = ['GDP', 'UNRATE', 'CPIAUCSL', 'FEDFUNDS']


class FredApiError(Exception):
    """Custom exception for FRED API errors."""
    pass


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((requests.RequestException, FredApiError))
)
def fetch_fred_data(endpoint: str, params: dict) -> dict:
    """
    Fetch data from FRED API with retry logic.

    Args:
        endpoint: API endpoint (e.g., 'series', 'series/observations')
        params: Query parameters

    Returns:
        JSON response as dictionary
    """
    url = f"{FRED_BASE_URL}/{endpoint}"
    params['api_key'] = FRED_API_KEY
    params['file_type'] = 'json'

    logger.debug(f"Fetching {url} with params {params}")

    response = requests.get(url, params=params, timeout=30)

    if response.status_code == 429:
        raise FredApiError("Rate limit exceeded")

    response.raise_for_status()
    data = response.json()

    if 'error_code' in data:
        raise FredApiError(f"FRED API error: {data.get('error_message', 'Unknown error')}")

    return data


def get_series_metadata(series_id: str) -> Optional[dict]:
    """Fetch metadata for a single series."""
    try:
        data = fetch_fred_data('series', {'series_id': series_id})
        if data.get('seriess'):
            return data['seriess'][0]
    except Exception as e:
        logger.error(f"Failed to fetch metadata for {series_id}: {e}")
    return None


def get_series_observations(
    series_id: str,
    observation_start: Optional[str] = None
) -> list[dict]:
    """
    Fetch observations for a series.

    Args:
        series_id: FRED series identifier
        observation_start: Optional start date for incremental load (YYYY-MM-DD)

    Returns:
        List of observation dictionaries
    """
    params = {'series_id': series_id}

    if observation_start:
        params['observation_start'] = observation_start

    try:
        data = fetch_fred_data('series/observations', params)
        return data.get('observations', [])
    except Exception as e:
        logger.error(f"Failed to fetch observations for {series_id}: {e}")
        return []


def init_database(conn: duckdb.DuckDBPyConnection) -> None:
    """Initialize database schema."""
    schema_path = Path(__file__).parent / 'schema.sql'
    if schema_path.exists():
        with open(schema_path) as f:
            conn.execute(f.read())
        logger.info("Schema initialized from schema.sql")
    else:
        # Inline schema if file not found
        conn.execute("""
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

            CREATE TABLE IF NOT EXISTS fred_observations (
                series_id VARCHAR NOT NULL,
                observation_date DATE NOT NULL,
                value DOUBLE,
                realtime_start DATE,
                realtime_end DATE,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (series_id, observation_date)
            );

            CREATE TABLE IF NOT EXISTS sync_metadata (
                source VARCHAR(50) PRIMARY KEY,
                last_sync_at TIMESTAMP,
                last_sync_status VARCHAR(20),
                records_synced INTEGER,
                error_message TEXT
            );
        """)
        logger.info("Schema initialized inline")


def get_last_observation_date(conn: duckdb.DuckDBPyConnection, series_id: str) -> Optional[str]:
    """Get the most recent observation date for a series (for incremental load)."""
    result = conn.execute("""
        SELECT MAX(observation_date) as max_date
        FROM fred_observations
        WHERE series_id = ?
    """, [series_id]).fetchone()

    if result and result[0]:
        return result[0].strftime('%Y-%m-%d')
    return None


def upsert_series(conn: duckdb.DuckDBPyConnection, metadata: dict) -> None:
    """Insert or update series metadata."""
    conn.execute("""
        INSERT OR REPLACE INTO fred_series (
            series_id, title, observation_start, observation_end,
            frequency, frequency_short, units, units_short,
            seasonal_adjustment, seasonal_adjustment_short,
            last_updated, popularity, notes, ingested_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """, [
        metadata['id'],
        metadata.get('title'),
        metadata.get('observation_start'),
        metadata.get('observation_end'),
        metadata.get('frequency'),
        metadata.get('frequency_short'),
        metadata.get('units'),
        metadata.get('units_short'),
        metadata.get('seasonal_adjustment'),
        metadata.get('seasonal_adjustment_short'),
        metadata.get('last_updated'),
        metadata.get('popularity'),
        metadata.get('notes')
    ])


def upsert_observations(
    conn: duckdb.DuckDBPyConnection,
    series_id: str,
    observations: list[dict]
) -> int:
    """
    Insert or update observations for a series.

    Returns:
        Number of records upserted
    """
    count = 0
    for obs in observations:
        # Skip observations with missing value (FRED uses '.' for missing)
        value_str = obs.get('value', '.')
        if value_str == '.' or not value_str:
            continue

        try:
            value = float(value_str)
        except ValueError:
            logger.warning(f"Invalid value for {series_id} on {obs.get('date')}: {value_str}")
            continue

        conn.execute("""
            INSERT OR REPLACE INTO fred_observations (
                series_id, observation_date, value,
                realtime_start, realtime_end, ingested_at
            ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, [
            series_id,
            obs['date'],
            value,
            obs.get('realtime_start'),
            obs.get('realtime_end')
        ])
        count += 1

    return count


def update_sync_metadata(
    conn: duckdb.DuckDBPyConnection,
    status: str,
    records: int,
    error: Optional[str] = None
) -> None:
    """Update sync tracking metadata."""
    conn.execute("""
        INSERT OR REPLACE INTO sync_metadata (
            source, last_sync_at, last_sync_status, records_synced, error_message
        ) VALUES ('fred', CURRENT_TIMESTAMP, ?, ?, ?)
    """, [status, records, error])


def ingest_fred_data(incremental: bool = True) -> None:
    """
    Main ingestion function.

    Args:
        incremental: If True, only fetch new observations since last sync
    """
    if not FRED_API_KEY:
        raise ValueError(
            "FRED_API_KEY environment variable not set. "
            "Get a free key at https://fred.stlouisfed.org/docs/api/api_key.html"
        )

    logger.info(f"Starting FRED ingestion (incremental={incremental})")
    logger.info(f"Database: {DB_PATH}")

    conn = duckdb.connect(str(DB_PATH))
    init_database(conn)

    total_records = 0

    try:
        for series_id in SERIES_IDS:
            logger.info(f"Processing series: {series_id}")

            # Fetch and store metadata
            metadata = get_series_metadata(series_id)
            if metadata:
                upsert_series(conn, metadata)
                logger.info(f"  Updated metadata: {metadata.get('title')}")

            # Determine start date for incremental load
            start_date = None
            if incremental:
                start_date = get_last_observation_date(conn, series_id)
                if start_date:
                    logger.info(f"  Incremental load from: {start_date}")

            # Fetch and store observations
            observations = get_series_observations(series_id, start_date)
            if observations:
                count = upsert_observations(conn, series_id, observations)
                total_records += count
                logger.info(f"  Loaded {count} observations")
            else:
                logger.info(f"  No new observations")

        update_sync_metadata(conn, 'success', total_records)
        logger.info(f"FRED ingestion complete. Total records: {total_records}")

    except Exception as e:
        update_sync_metadata(conn, 'failed', total_records, str(e))
        logger.error(f"FRED ingestion failed: {e}")
        raise

    finally:
        conn.close()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Ingest FRED economic data')
    parser.add_argument(
        '--full',
        action='store_true',
        help='Run full load instead of incremental'
    )
    args = parser.parse_args()

    ingest_fred_data(incremental=not args.full)
