#!/usr/bin/env python3
"""
Quick demo script for the API → Warehouse POC.

Runs HN ingestion (no API key needed) and shows results.
For FRED, you'll need to set FRED_API_KEY in .env
"""

import os
import sys
from pathlib import Path

# Add solution directory to path
sys.path.insert(0, str(Path(__file__).parent))


def main():
    import duckdb

    db_path = Path(__file__).parent / 'warehouse.duckdb'

    print("=" * 60)
    print("API → Warehouse POC Demo")
    print("=" * 60)

    # Check if FRED API key is set
    fred_key = os.getenv('FRED_API_KEY')

    # Run HN ingestion first (no auth needed)
    print("\n[1] Hacker News Ingestion (no API key needed)")
    print("-" * 40)

    from ingest_hn import ingest_hn_data
    ingest_hn_data(stories_limit=10, fetch_comments=True, fetch_users=False)

    # Try FRED if key is available
    if fred_key:
        print("\n[2] FRED Ingestion (API key found)")
        print("-" * 40)
        from ingest_fred import ingest_fred_data
        ingest_fred_data(incremental=True)
    else:
        print("\n[2] FRED Ingestion (SKIPPED - no API key)")
        print("-" * 40)
        print("Set FRED_API_KEY in .env to enable FRED ingestion")
        print("Get a free key at: https://fred.stlouisfed.org/docs/api/api_key.html")

    # Show results
    print("\n" + "=" * 60)
    print("Results")
    print("=" * 60)

    conn = duckdb.connect(str(db_path))

    print("\n[HN] Item counts by type:")
    print(conn.execute("""
        SELECT type, COUNT(*) as count
        FROM hn_items
        GROUP BY type
        ORDER BY count DESC
    """).fetchdf().to_string(index=False))

    print("\n[HN] Top 5 stories by comment count:")
    print(conn.execute("""
        SELECT title, descendants as comments, score
        FROM hn_items
        WHERE type = 'story'
        ORDER BY descendants DESC NULLS LAST
        LIMIT 5
    """).fetchdf().to_string(index=False))

    # Check FRED data
    fred_count = conn.execute("SELECT COUNT(*) FROM fred_series").fetchone()[0]
    if fred_count > 0:
        print("\n[FRED] Series ingested:")
        print(conn.execute("""
            SELECT series_id, title, units
            FROM fred_series
        """).fetchdf().to_string(index=False))

        print("\n[FRED] Latest observations:")
        print(conn.execute("""
            SELECT
                s.series_id,
                MAX(o.observation_date) as latest_date,
                o.value as latest_value
            FROM fred_series s
            JOIN fred_observations o ON s.series_id = o.series_id
            WHERE (s.series_id, o.observation_date) IN (
                SELECT series_id, MAX(observation_date)
                FROM fred_observations
                GROUP BY series_id
            )
            GROUP BY s.series_id, o.value
        """).fetchdf().to_string(index=False))

    print("\n[Sync] Metadata:")
    print(conn.execute("SELECT * FROM sync_metadata").fetchdf().to_string(index=False))

    conn.close()

    print("\n" + "=" * 60)
    print(f"Database: {db_path}")
    print("=" * 60)


if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()
    main()
