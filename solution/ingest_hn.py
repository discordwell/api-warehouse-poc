#!/usr/bin/env python3
"""
Hacker News Ingestion Script

Fetches top stories, comments, and user data from the HN Firebase API
and loads it into DuckDB. Handles recursive comment fetching.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional
from collections import deque

import duckdb
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
HN_BASE_URL = 'https://hacker-news.firebaseio.com/v0'
DB_PATH = Path(__file__).parent / 'warehouse.duckdb'

# Ingestion settings
TOP_STORIES_LIMIT = 100
MAX_COMMENT_DEPTH = 10  # Prevent infinite recursion
REQUEST_DELAY = 0.1  # Seconds between requests (be nice to the API)
MAX_WORKERS = 5  # Concurrent API requests


class HNApiError(Exception):
    """Custom exception for HN API errors."""
    pass


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=5),
    retry=retry_if_exception_type(requests.RequestException)
)
def fetch_hn_data(endpoint: str) -> Optional[dict | list]:
    """
    Fetch data from Hacker News API with retry logic.

    Args:
        endpoint: API endpoint path (e.g., 'topstories', 'item/123')

    Returns:
        JSON response or None if item doesn't exist
    """
    url = f"{HN_BASE_URL}/{endpoint}.json"

    response = requests.get(url, timeout=10)
    response.raise_for_status()

    data = response.json()
    return data


def get_top_stories(limit: int = TOP_STORIES_LIMIT) -> list[int]:
    """Fetch IDs of top stories."""
    logger.info(f"Fetching top {limit} stories")
    story_ids = fetch_hn_data('topstories')

    if story_ids:
        return story_ids[:limit]
    return []


def get_item(item_id: int) -> Optional[dict]:
    """Fetch a single item (story, comment, etc.)."""
    try:
        return fetch_hn_data(f'item/{item_id}')
    except Exception as e:
        logger.warning(f"Failed to fetch item {item_id}: {e}")
        return None


def get_user(user_id: str) -> Optional[dict]:
    """Fetch a user profile."""
    try:
        return fetch_hn_data(f'user/{user_id}')
    except Exception as e:
        logger.warning(f"Failed to fetch user {user_id}: {e}")
        return None


def fetch_items_parallel(item_ids: list[int]) -> list[dict]:
    """
    Fetch multiple items in parallel.

    Args:
        item_ids: List of item IDs to fetch

    Returns:
        List of successfully fetched items
    """
    items = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_id = {
            executor.submit(get_item, item_id): item_id
            for item_id in item_ids
        }

        for future in as_completed(future_to_id):
            item_id = future_to_id[future]
            try:
                item = future.result()
                if item:
                    items.append(item)
            except Exception as e:
                logger.warning(f"Error fetching item {item_id}: {e}")

    return items


def fetch_comments_recursive(
    root_item: dict,
    max_depth: int = MAX_COMMENT_DEPTH
) -> list[dict]:
    """
    Fetch all comments for a story using BFS (breadth-first).

    Args:
        root_item: The story item
        max_depth: Maximum depth to traverse

    Returns:
        List of all comment items
    """
    comments = []
    queue = deque()  # (kid_ids, current_depth)

    # Start with direct children of the story
    kids = root_item.get('kids', [])
    if kids:
        queue.append((kids, 1))

    while queue:
        kid_ids, depth = queue.popleft()

        if depth > max_depth:
            logger.warning(f"Reached max comment depth {max_depth}")
            continue

        # Fetch this batch of comments
        batch = fetch_items_parallel(kid_ids)

        for comment in batch:
            if comment and comment.get('type') == 'comment':
                comments.append(comment)

                # Queue grandchildren for fetching
                grandkids = comment.get('kids', [])
                if grandkids and depth < max_depth:
                    queue.append((grandkids, depth + 1))

        # Rate limiting between batches
        time.sleep(REQUEST_DELAY)

    return comments


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
            CREATE TABLE IF NOT EXISTS hn_items (
                id INTEGER PRIMARY KEY,
                type VARCHAR(20),
                "by" VARCHAR(100),
                time INTEGER,
                text TEXT,
                parent INTEGER,
                url VARCHAR(2000),
                score INTEGER,
                title VARCHAR(500),
                descendants INTEGER,
                dead BOOLEAN DEFAULT FALSE,
                deleted BOOLEAN DEFAULT FALSE,
                poll INTEGER,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS hn_item_kids (
                item_id INTEGER NOT NULL,
                kid_id INTEGER NOT NULL,
                position INTEGER,
                PRIMARY KEY (item_id, kid_id)
            );

            CREATE TABLE IF NOT EXISTS hn_users (
                id VARCHAR(100) PRIMARY KEY,
                created INTEGER,
                karma INTEGER,
                about TEXT,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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


def item_exists(conn: duckdb.DuckDBPyConnection, item_id: int) -> bool:
    """Check if an item already exists in the database."""
    result = conn.execute(
        "SELECT 1 FROM hn_items WHERE id = ?", [item_id]
    ).fetchone()
    return result is not None


def upsert_item(conn: duckdb.DuckDBPyConnection, item: dict) -> None:
    """Insert or update an item."""
    conn.execute("""
        INSERT OR REPLACE INTO hn_items (
            id, type, "by", time, text, parent, url, score,
            title, descendants, dead, deleted, poll, ingested_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """, [
        item['id'],
        item.get('type'),
        item.get('by'),
        item.get('time'),
        item.get('text'),
        item.get('parent'),
        item.get('url'),
        item.get('score'),
        item.get('title'),
        item.get('descendants'),
        item.get('dead', False),
        item.get('deleted', False),
        item.get('poll')
    ])

    # Store kids relationships
    kids = item.get('kids', [])
    for position, kid_id in enumerate(kids):
        conn.execute("""
            INSERT OR REPLACE INTO hn_item_kids (item_id, kid_id, position)
            VALUES (?, ?, ?)
        """, [item['id'], kid_id, position])


def upsert_user(conn: duckdb.DuckDBPyConnection, user: dict) -> None:
    """Insert or update a user."""
    conn.execute("""
        INSERT OR REPLACE INTO hn_users (
            id, created, karma, about, ingested_at
        ) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
    """, [
        user['id'],
        user.get('created'),
        user.get('karma'),
        user.get('about')
    ])


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
        ) VALUES ('hacker_news', CURRENT_TIMESTAMP, ?, ?, ?)
    """, [status, records, error])


def ingest_hn_data(
    stories_limit: int = TOP_STORIES_LIMIT,
    fetch_comments: bool = True,
    fetch_users: bool = True
) -> None:
    """
    Main ingestion function.

    Args:
        stories_limit: Number of top stories to fetch
        fetch_comments: Whether to fetch comments for each story
        fetch_users: Whether to fetch user profiles for story authors
    """
    logger.info(f"Starting Hacker News ingestion")
    logger.info(f"Database: {DB_PATH}")
    logger.info(f"Stories limit: {stories_limit}, Comments: {fetch_comments}, Users: {fetch_users}")

    conn = duckdb.connect(str(DB_PATH))
    init_database(conn)

    total_items = 0
    total_users = 0
    authors = set()

    try:
        # Fetch top story IDs
        story_ids = get_top_stories(stories_limit)
        logger.info(f"Got {len(story_ids)} story IDs")

        # Fetch stories in parallel
        stories = fetch_items_parallel(story_ids)
        logger.info(f"Fetched {len(stories)} stories")

        for story in stories:
            # Skip if already ingested (for incremental loads)
            if item_exists(conn, story['id']):
                logger.debug(f"Story {story['id']} already exists, updating...")

            # Store the story
            upsert_item(conn, story)
            total_items += 1

            # Track author for user fetch
            if story.get('by'):
                authors.add(story['by'])

            # Fetch comments for this story
            if fetch_comments and story.get('kids'):
                logger.info(f"  Fetching comments for story {story['id']}: {story.get('title', '')[:50]}...")
                comments = fetch_comments_recursive(story)
                logger.info(f"    Found {len(comments)} comments")

                for comment in comments:
                    upsert_item(conn, comment)
                    total_items += 1

                    # Track comment authors
                    if comment.get('by'):
                        authors.add(comment['by'])

        # Fetch user profiles
        if fetch_users and authors:
            logger.info(f"Fetching {len(authors)} user profiles")
            for username in authors:
                user = get_user(username)
                if user:
                    upsert_user(conn, user)
                    total_users += 1
                time.sleep(REQUEST_DELAY)

            logger.info(f"Fetched {total_users} users")

        update_sync_metadata(conn, 'success', total_items)
        logger.info(f"Hacker News ingestion complete. Items: {total_items}, Users: {total_users}")

    except Exception as e:
        update_sync_metadata(conn, 'failed', total_items, str(e))
        logger.error(f"Hacker News ingestion failed: {e}")
        raise

    finally:
        conn.close()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Ingest Hacker News data')
    parser.add_argument(
        '--limit',
        type=int,
        default=TOP_STORIES_LIMIT,
        help=f'Number of top stories to fetch (default: {TOP_STORIES_LIMIT})'
    )
    parser.add_argument(
        '--no-comments',
        action='store_true',
        help='Skip fetching comments'
    )
    parser.add_argument(
        '--no-users',
        action='store_true',
        help='Skip fetching user profiles'
    )
    args = parser.parse_args()

    ingest_hn_data(
        stories_limit=args.limit,
        fetch_comments=not args.no_comments,
        fetch_users=not args.no_users
    )
