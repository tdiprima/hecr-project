# data-audit

One-off scripts used to investigate and repair a data discrepancy: faculty authors flagged as having no publications actually had publications in the database. These scripts identify those authors, fix missing metadata, and export their publication records.

## Scripts

Run them in this order:

| Script | What it does |
|---|---|
| `check_publications_in_db.py` | Cross-references `profiles_without_publications.csv` against the DB; saves matches to `authors_with_pubs_found.csv` |
| `update_departments.py` | Backfills empty department fields for records sourced from `grace` |
| `add_name_columns.py` | Splits `db_name` into separate `Lastname`/`Firstname` columns (handles both `grace` and `local` name formats) |
| `export_author_publications.py` | Exports all publication records for the matched authors to `author_publications_export.csv` |

## Requirements

Set `DATABASE_URL` in a `.env` file in the project root before running scripts that query the database.

```
DATABASE_URL=postgresql://user:password@host/dbname
```

<br>
