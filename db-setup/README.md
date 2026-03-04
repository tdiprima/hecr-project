# interfolio-ingest

**Run this first.** Everything downstream depends on the database being set up and populated.

## What it does

1. **`create_db.py`** — Creates the PostgreSQL tables (`users`, `publications`, `grants`) using SQLAlchemy models.
2. **`collect_activities.py`** — Pulls publications (Journal Articles, Books) and grants for all faculty users from the Interfolio Faculty180 API and stores them in the database.

## Order of operations

```
create_db.py → collect_activities.py
```

`collect_activities.py` queries the `users` table to get IDs, so the DB must exist first.

## Setup

Copy `.env` and fill in your credentials:

```
DATABASE_URL=postgresql://user:pass@host/dbname
API_PUBLIC_KEY=...
API_PRIVATE_KEY=...
TENANT_1_DATABASE_ID=...
```

## Usage

```bash
python create_db.py
python collect_activities.py [--workers 12] [--batch 50] [--verbose]
```

Use `--batch N` to test with a small subset of users before running against everyone.

<br>
