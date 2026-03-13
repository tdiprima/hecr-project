# Health Equity and Climate Research (HECR) Pipeline

Identifies faculty researchers working on health equity and climate health topics by mining Interfolio Faculty180 data.

---

## Problem

Finding researchers whose work intersects health equity and climate medicine requires manually reviewing hundreds of faculty profiles, publications, and grants — a process that doesn't scale and goes stale quickly.

## Solution

A four-phase Python pipeline that:

1. **Ingests** faculty publications and grants from the Interfolio Faculty180 API into PostgreSQL
2. **Identifies** researchers via keyword matching across health equity, climate health, and intersectional terms
3. **Syncs** data on a schedule (upsert + stale-record deletion) to stay current
4. **Audits** data quality and exports clean records for downstream use

```
db-setup → researcher-identification → interfolio_data_sync (recurring) → data-audit (as needed)
```

## Example Output

`hecr_export.csv` — one row per researcher/publication/grant combination:

```
firstname,lastname,user_id,publication_title,publication_id,grant_title,grant_id
Jane,Smith,1042,Climate change and respiratory disparities in urban populations,8821,,
John,Doe,2301,,,"NIH R01: Heat stress in underserved rural communities",412
```

## Installation

**Requirements:** Python 3.9+, PostgreSQL

```bash
git clone https://github.com/tdiprima/hecr-project.git
cd hecr-project
pip install -r requirements.txt
```

Copy `.env.sample` to `.env` and fill in your credentials:

```
DATABASE_URL=postgresql://user:pass@host/dbname
API_PUBLIC_KEY=...
API_PRIVATE_KEY=...
TENANT_1_DATABASE_ID=...
DB_USER=...
DB_PASS=...
```

## Usage

Run phases in order:

```bash
# Phase 1 — create schema and ingest data
python db-setup/create_db.py
python db-setup/collect_activities.py [--workers 12] [--batch 50] [--verbose]

# Phase 2 — identify HECR researchers
python researcher-identification/hecr_researcher_identifier.py
python researcher-identification/hecr_csv_export.py

# Phase 3 — ongoing sync (run nightly or weekly)
python interfolio_data_sync/collect_activities_improved.py [--workers 8] [--batch 100]

# Phase 4 — data audit (as needed)
python data-audit/check_publications_in_db.py
python data-audit/update_departments.py
python data-audit/add_name_columns.py
python data-audit/export_author_publications.py
```

---

## Disclaimer

This pipeline requires valid Interfolio Faculty180 API credentials and an institutional database. It is intended for authorized use by staff with access to faculty profile data. Do not store credentials in version control — use the `.env` file, which is excluded from git.

<br>
