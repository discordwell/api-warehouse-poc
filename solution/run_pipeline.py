#!/usr/bin/env python3
"""
Orchestration script for the API → Warehouse pipeline.

Runs both FRED and Hacker News ingestion in sequence.
"""

import argparse
import logging
import sys
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description='Run the API → Warehouse data pipeline'
    )
    parser.add_argument(
        '--fred-only',
        action='store_true',
        help='Only run FRED ingestion'
    )
    parser.add_argument(
        '--hn-only',
        action='store_true',
        help='Only run Hacker News ingestion'
    )
    parser.add_argument(
        '--full',
        action='store_true',
        help='Run full load instead of incremental (FRED)'
    )
    parser.add_argument(
        '--hn-limit',
        type=int,
        default=100,
        help='Number of HN stories to fetch (default: 100)'
    )
    parser.add_argument(
        '--no-comments',
        action='store_true',
        help='Skip fetching HN comments'
    )
    parser.add_argument(
        '--verify',
        action='store_true',
        help='Run verification queries after ingestion'
    )
    args = parser.parse_args()

    success = True

    # Run FRED ingestion
    if not args.hn_only:
        try:
            logger.info("=" * 60)
            logger.info("Starting FRED ingestion")
            logger.info("=" * 60)
            from ingest_fred import ingest_fred_data
            ingest_fred_data(incremental=not args.full)
        except Exception as e:
            logger.error(f"FRED ingestion failed: {e}")
            success = False

    # Run Hacker News ingestion
    if not args.fred_only:
        try:
            logger.info("=" * 60)
            logger.info("Starting Hacker News ingestion")
            logger.info("=" * 60)
            from ingest_hn import ingest_hn_data
            ingest_hn_data(
                stories_limit=args.hn_limit,
                fetch_comments=not args.no_comments
            )
        except Exception as e:
            logger.error(f"Hacker News ingestion failed: {e}")
            success = False

    # Run verification
    if args.verify and success:
        try:
            logger.info("=" * 60)
            logger.info("Running verification queries")
            logger.info("=" * 60)
            run_verification()
        except Exception as e:
            logger.error(f"Verification failed: {e}")
            success = False

    if success:
        logger.info("=" * 60)
        logger.info("Pipeline completed successfully!")
        logger.info("=" * 60)
        return 0
    else:
        logger.error("Pipeline completed with errors")
        return 1


def run_verification():
    """Run verification queries and print results."""
    import duckdb

    db_path = Path(__file__).parent / 'warehouse.duckdb'
    verify_path = Path(__file__).parent.parent / 'verify' / 'test_queries.sql'

    conn = duckdb.connect(str(db_path))

    # Run a few key verification queries
    queries = [
        ("FRED Series Count",
         "SELECT COUNT(DISTINCT series_id) as series_count FROM fred_series"),

        ("FRED Observations Count",
         "SELECT series_id, COUNT(*) as obs_count FROM fred_observations GROUP BY series_id"),

        ("HN Item Types",
         "SELECT type, COUNT(*) as count FROM hn_items GROUP BY type"),

        ("HN Story Count",
         "SELECT COUNT(*) as story_count FROM hn_items WHERE type = 'story'"),

        ("Sync Status",
         "SELECT * FROM sync_metadata"),
    ]

    for name, query in queries:
        try:
            result = conn.execute(query).fetchdf()
            logger.info(f"\n{name}:")
            print(result.to_string(index=False))
        except Exception as e:
            logger.warning(f"Query '{name}' failed: {e}")

    conn.close()


if __name__ == '__main__':
    sys.exit(main())
