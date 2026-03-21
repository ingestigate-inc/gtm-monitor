# GTM Monitor

Private market research tool for Ingestigate (https://ingestigate.com).

## What it does

Reads recent public posts from a list of subreddits related to our industry (document management, eDiscovery, digital forensics, fraud investigation). Results are stored in a private PostgreSQL database for internal review.

## What it does NOT do

- No posting, commenting, or voting
- No direct messaging
- No data redistribution or publication
- No scraping of user profiles
- No access to private/quarantined content beyond what the authenticated account can see as a member

## How it works

- `scripts/fetch.py` — Pulls recent posts from configured subreddits via Reddit's JSON API. Rate-limited to one request every 2+ seconds.
- `config.json` — Lists target subreddits and the business context for monitoring each one.

## Usage

Runs a few times per day on a private server. Single developer account. Script app type.

## Author

Adam Rutkowski — Founder, Ingestigate
