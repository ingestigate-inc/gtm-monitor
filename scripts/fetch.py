#!/usr/bin/env python3
"""
GTM Monitor — Fetch Script

Pulls latest posts from target Reddit subreddits and runs Brave web searches.
Stages all results in gtm.raw_findings for LLM scoring.

Usage:
    python3 fetch.py                    # Run all due topics
    python3 fetch.py --force            # Run all regardless of schedule
    python3 fetch.py --reddit-only      # Only fetch Reddit
    python3 fetch.py --brave-only       # Only fetch Brave
    python3 fetch.py --verbose          # Show progress
"""

import argparse
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests as http_client
import psycopg2

# Load .env file from project root
ENV_FILE = Path(__file__).parent.parent / ".env"
if ENV_FILE.exists():
    with open(ENV_FILE) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())

# Database connection — reads from .env or environment
DB_HOST = os.environ.get("PG_DATABASE_HOST", "localhost")
DB_PORT = os.environ.get("PG_DATABASE_PORT", "5432")
DB_NAME = os.environ.get("PG_DATABASE_NAME", "default")
DB_USER = os.environ.get("PG_DATABASE_USER", "twenty")
DB_PASS = os.environ.get("PG_DATABASE_PASSWORD", "")

CONFIG_PATH = Path(__file__).parent.parent / "config.json"


def get_db():
    """Get a psycopg2 connection to twenty-db."""
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )


def load_config():
    """Load config.json."""
    with open(CONFIG_PATH) as f:
        return json.load(f)


def source_id_hash(url):
    """Create a stable hash for dedup."""
    return hashlib.md5(url.encode()).hexdigest()


def is_duplicate(conn, source_id):
    """Check if we've already fetched this source."""
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM gtm.raw_findings WHERE source_id = %s LIMIT 1", (source_id,))
    result = cur.fetchone()
    cur.close()
    return result is not None


def insert_raw_finding(conn, finding):
    """Insert a raw finding into staging table."""
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO gtm.raw_findings
            (source, source_id, subreddit, url, title, content, author,
             score, num_comments, post_age_hours)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        finding["source"],
        finding["source_id"],
        finding.get("subreddit"),
        finding["url"],
        finding["title"],
        finding.get("content", ""),
        finding.get("author"),
        finding.get("score"),
        finding.get("num_comments"),
        finding.get("post_age_hours"),
    ))
    conn.commit()
    cur.close()


def fetch_reddit_subreddit(subreddit_config, settings, conn, verbose=False):
    """Fetch recent posts from a subreddit via Reddit JSON API."""
    name = subreddit_config["name"]
    pull_count = subreddit_config.get("pull_count", 25)
    user_agent = settings.get("reddit_user_agent", "script:gtm-monitor:v1.0.0")
    delay_ms = settings.get("reddit_request_delay_ms", 1500)

    url = f"https://www.reddit.com/r/{name}/new.json?limit={pull_count}"

    time.sleep(delay_ms / 1000.0)

    try:
        resp = http_client.get(
            url,
            headers={"User-Agent": user_agent, "Accept": "application/json"},
            timeout=20
        )
        resp.raise_for_status()

        if resp.text.strip().startswith("<"):
            if verbose:
                print(f"  WARNING: r/{name} returned HTML, skipping")
            return 0

        data = resp.json()
    except Exception as e:
        print(f"  ERROR fetching r/{name}: {e}", file=sys.stderr)
        return 0

    posts = data.get("data", {}).get("children", [])
    new_count = 0
    now = datetime.now(timezone.utc)

    for post in posts:
        if post.get("kind") != "t3":
            continue
        d = post["data"]

        post_url = f"https://www.reddit.com{d.get('permalink', '')}"
        sid = d.get("id", source_id_hash(post_url))

        if is_duplicate(conn, sid):
            continue

        created_utc = d.get("created_utc", 0)
        age_hours = (now - datetime.fromtimestamp(created_utc, tz=timezone.utc)).total_seconds() / 3600

        selftext = d.get("selftext", "") or ""
        if len(selftext) > 2000:
            selftext = selftext[:2000] + "..."

        insert_raw_finding(conn, {
            "source": "reddit",
            "source_id": sid,
            "subreddit": name,
            "url": post_url,
            "title": d.get("title", ""),
            "content": selftext,
            "author": d.get("author"),
            "score": d.get("score", 0),
            "num_comments": d.get("num_comments", 0),
            "post_age_hours": round(age_hours, 1),
        })
        new_count += 1

    if verbose:
        print(f"  r/{name}: {len(posts)} posts fetched, {new_count} new")
    return new_count


def fetch_brave_topic(topic_config, settings, conn, verbose=False):
    """Search Brave Web Search API for a topic."""
    api_key = os.environ.get("BRAVE_SEARCH_API_KEY")
    if not api_key:
        if verbose:
            print("  SKIP Brave: BRAVE_SEARCH_API_KEY not set")
        return 0

    query = topic_config["query"]
    freshness = topic_config.get("freshness", "pw")
    max_results = topic_config.get("max_results", 10)

    params = {
        "q": query,
        "count": min(max_results, 20),
    }
    if freshness:
        params["freshness"] = freshness

    try:
        resp = http_client.get(
            "https://api.search.brave.com/res/v1/web/search",
            params=params,
            headers={
                "Accept": "application/json",
                "x-subscription-token": api_key,
            },
            timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  ERROR Brave search '{topic_config['id']}': {e}", file=sys.stderr)
        return 0

    web_results = data.get("web", {}).get("results", [])
    new_count = 0

    for item in web_results:
        item_url = item.get("url", "")
        sid = source_id_hash(item_url)

        if is_duplicate(conn, sid):
            continue

        insert_raw_finding(conn, {
            "source": "brave_web",
            "source_id": sid,
            "subreddit": None,
            "url": item_url,
            "title": item.get("title", ""),
            "content": item.get("description", ""),
            "author": None,
            "score": None,
            "num_comments": None,
            "post_age_hours": None,
        })
        new_count += 1

    if verbose:
        print(f"  Brave '{topic_config['id']}': {len(web_results)} results, {new_count} new")
    return new_count


def cleanup_old_findings(conn, max_age_days=30):
    """Remove old unscored raw findings."""
    cur = conn.cursor()
    cur.execute("""
        DELETE FROM gtm.raw_findings
        WHERE scored = FALSE
          AND promoted = FALSE
          AND fetched_at < NOW() - INTERVAL '%s days'
    """, (max_age_days,))
    deleted = cur.rowcount
    conn.commit()
    cur.close()
    return deleted


def main():
    parser = argparse.ArgumentParser(description="GTM Monitor — Fetch")
    parser.add_argument("--force", action="store_true", help="Run all topics regardless of schedule")
    parser.add_argument("--reddit-only", action="store_true", help="Only fetch Reddit")
    parser.add_argument("--brave-only", action="store_true", help="Only fetch Brave")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show progress")
    args = parser.parse_args()

    config = load_config()
    settings = config.get("settings", {})
    conn = get_db()

    total_new = 0

    # Reddit subreddits
    if not args.brave_only:
        subreddits = config.get("reddit_subreddits", [])
        if args.verbose:
            print(f"Fetching {len(subreddits)} subreddits...")
        for sub in subreddits:
            try:
                n = fetch_reddit_subreddit(sub, settings, conn, verbose=args.verbose)
                total_new += n
            except Exception as e:
                print(f"  ERROR r/{sub['name']}: {e}", file=sys.stderr)

    # Brave web topics
    if not args.reddit_only:
        topics = config.get("brave_web_topics", [])
        if args.verbose:
            print(f"Searching {len(topics)} Brave topics...")
        for topic in topics:
            try:
                n = fetch_brave_topic(topic, settings, conn, verbose=args.verbose)
                total_new += n
            except Exception as e:
                print(f"  ERROR Brave '{topic['id']}': {e}", file=sys.stderr)

    # Cleanup old unscored findings
    max_age = settings.get("max_raw_findings_age_days", 30)
    deleted = cleanup_old_findings(conn, max_age)

    if args.verbose:
        print(f"\nDone. {total_new} new findings staged. {deleted} old findings cleaned up.")

    # Summary for non-verbose mode
    if not args.verbose and total_new > 0:
        print(f"{total_new} new findings staged")

    conn.close()


if __name__ == "__main__":
    main()
