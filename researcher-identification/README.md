# researcher-identification

**Why this runs second:** Phase 1 (db-setup) must run first to build the `research` database and populate the `users`, `publications`, and `grants` tables from Interfolio. This phase queries those tables — it has nothing to work with otherwise.

## What it does

Identifies researchers whose publications or grants match health equity and climate health keywords, writes them into a dedicated `hecr` table, and exports results to CSV.

## Scripts

| Script | Purpose |
|---|---|
| `hecr_researcher_identifier.py` | Core logic — keyword search across publications/grants, creates and populates the `hecr` table |
| `hecr_csv_export.py` | Exports `hecr` table (researchers + their publications/grants) to `hecr_export.csv` |
| `quick_pandas.py` | Same export, but via pandas — quick sanity check |
| `query_users.py` | Lists all users who have any publications or grants (pre-filter utility) |

## Usage

```bash
# Identify researchers and build the hecr table
python hecr_researcher_identifier.py

# Export results
python hecr_csv_export.py
```

## Requirements

- PostgreSQL `research` database with `users`, `publications`, and `grants` tables (from Phase 1)
- `.env` with `DB_USER` and `DB_PASS`

## Keywords searched

- **Health equity:** health disparities, healthcare access, racial/ethnic disparities, social determinants, underserved populations, rural/urban health, etc.
- **Climate health:** climate change, planetary health, environmental health, heat stress, air pollution, vector-borne disease, etc.
- **Intersection:** climate justice, environmental justice, climate equity, pollution disparities, etc.
