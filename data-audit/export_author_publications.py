"""
Script to export publications for authors from authors_with_pubs_found.csv

Reads the CSV file, looks up each author in the database, and exports
all their publication data (excluding id) along with author information.

Usage:
    python export_author_publications.py
"""

import csv
import os
import sys
from pathlib import Path

# Add parent directory to path to import from interfolio_data_sync
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from interfolio_data_sync.models import Publication, User
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def load_csv_authors(csv_path):
    """Load authors from the CSV file"""
    authors = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            authors.append(
                {
                    "lastname": row["Lastname"],
                    "firstname": row["Firstname"],
                    "departments": row["departments"],
                }
            )
    return authors


def export_author_publications(csv_path, output_path):
    """Export publications for authors from CSV"""
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise ValueError(
            "DATABASE_URL not found in environment. Make sure .env file exists and contains DATABASE_URL"
        )

    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Load authors from CSV
        csv_authors = load_csv_authors(csv_path)
        print(f"Loaded {len(csv_authors)} authors from CSV\n")

        # Prepare output data
        output_rows = []

        for author in csv_authors:
            # Find user in database
            user = (
                session.query(User)
                .filter(
                    User.lastname.ilike(author["lastname"]),
                    User.firstname.ilike(author["firstname"]),
                )
                .first()
            )

            if not user:
                print(
                    f"Warning: User not found - {author['lastname']}, {author['firstname']}"
                )
                continue

            # Get all publications for this user
            publications = (
                session.query(Publication).filter(Publication.user_id == user.id).all()
            )

            if not publications:
                print(
                    f"No publications found for {author['lastname']}, {author['firstname']}"
                )
                continue

            print(
                f"Found {len(publications)} publications for {author['lastname']}, {author['firstname']}"
            )

            # Create a row for each publication
            for pub in publications:
                row = {
                    "lastname": author["lastname"],
                    "firstname": author["firstname"],
                    "department": author["departments"],
                    # Publication fields (excluding id)
                    "user_id": pub.user_id,
                    "activityid": pub.activityid,
                    "type": pub.type,
                    "title": pub.title,
                    "journal": pub.journal,
                    "series_title": pub.series_title,
                    "year": pub.year,
                    "month_season": pub.month_season,
                    "publisher": pub.publisher,
                    "publisher_city_state": pub.publisher_city_state,
                    "publisher_country": pub.publisher_country,
                    "volume": pub.volume,
                    "issue_number": pub.issue_number,
                    "page_numbers": pub.page_numbers,
                    "isbn": pub.isbn,
                    "issn": pub.issn,
                    "doi": pub.doi,
                    "url": pub.url,
                    "description": pub.description,
                    "origin": pub.origin,
                    "status": pub.status,
                    "term": pub.term,
                    "status_year": pub.status_year,
                }
                output_rows.append(row)

        # Write to CSV
        if output_rows:
            fieldnames = [
                "lastname",
                "firstname",
                "department",
                "user_id",
                "activityid",
                "type",
                "title",
                "journal",
                "series_title",
                "year",
                "month_season",
                "publisher",
                "publisher_city_state",
                "publisher_country",
                "volume",
                "issue_number",
                "page_numbers",
                "isbn",
                "issn",
                "doi",
                "url",
                "description",
                "origin",
                "status",
                "term",
                "status_year",
            ]

            with open(output_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(output_rows)

            print("\n" + "=" * 80)
            print(f"Successfully exported {len(output_rows)} publication records")
            print(f"Output file: {output_path}")
            print("=" * 80)
        else:
            print("\nNo publications found to export")

    finally:
        session.close()


def main():
    csv_path = "authors_with_pubs_found.csv"
    output_path = "author_publications_export.csv"

    # Check if CSV exists
    if not Path(csv_path).exists():
        print(f"Error: {csv_path} not found!")
        print(f"Current directory: {Path.cwd()}")
        return

    export_author_publications(csv_path, output_path)


if __name__ == "__main__":
    main()
