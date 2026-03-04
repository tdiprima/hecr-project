# activity_sync

**Run this third (and on a schedule).** Keeps publications and grants in sync with Interfolio after initial setup.

Unlike the one-time ingest in `db-setup`, this script adds new records, updates changed ones, and deletes stale ones no longer returned by the API. Run it nightly or weekly to keep data current.

## What it does

- Fetches publications (`/activities/-21`) and grants (`/activities/-11`) for all non-staff faculty from Interfolio Faculty180
- Upserts records into the `publications` and `grants` tables
- Deletes records that no longer appear in the API
- Processes users in parallel (threaded) with retry/rate-limit handling

## Prerequisites

1. `db-setup` has been run (schema exists, `users` table is populated)
2. `researcher-identification` has been run (optional, but this is the intended third step)

## Setup

Same `.env` as `db-setup`:

```
DATABASE_URL=postgresql://user:pass@host/dbname
API_PUBLIC_KEY=...
API_PRIVATE_KEY=...
TENANT_1_DATABASE_ID=...
```

## Usage

```bash
# Full sync
python collect_activities_improved.py

# Test with 100 users
python collect_activities_improved.py --batch 100 --verbose

# Throttle if rate-limited
python collect_activities_improved.py --workers 8
```

## Files

| File | Purpose |
|---|---|
| `collect_activities_improved.py` | Main sync script |
| `activity_utils.py` | Helpers: upsert logic, stale-record deletion, API→model mapping |
| `models.py` | SQLAlchemy models for `User`, `Publication`, `Grant` |
