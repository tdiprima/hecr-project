"""
Script to check which authors from profiles_without_publications.csv
actually have publications in the database.

This helps identify discrepancies between the CSV report and the actual database state.

Usage:
    python check_publications_in_db.py
"""

import csv
import operator
import os
import sys
from pathlib import Path

# Add parent directory to path to import from interfolio_data_sync
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from interfolio_data_sync.models import Publication, User
from sqlalchemy import create_engine, func
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
                    "departments": row["Departments"],
                }
            )
    return authors


def check_authors_with_publications(csv_path):
    """Check which CSV authors have publications in the database"""
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

        # Get all users with their publication counts
        users_with_pubs = (
            session.query(
                User.id,
                User.firstname,
                User.lastname,
                func.count(Publication.id).label("pub_count"),
            )
            .join(Publication, User.id == Publication.user_id)
            .group_by(User.id, User.firstname, User.lastname)
            .all()
        )

        # Create a lookup dict by name
        users_dict = {}
        for user in users_with_pubs:
            # Create key with normalized names (lowercase, stripped)
            key = (
                user.lastname.lower().strip() if user.lastname else "",
                user.firstname.lower().strip() if user.firstname else "",
            )
            users_dict[key] = {
                "user_id": user.id,
                "firstname": user.firstname,
                "lastname": user.lastname,
                "pub_count": user.pub_count,
            }

        # Find matches
        authors_with_pubs = []
        for author in csv_authors:
            key = (
                author["lastname"].lower().strip(),
                author["firstname"].lower().strip(),
            )

            if key in users_dict:
                user_data = users_dict[key]
                authors_with_pubs.append(
                    {
                        "csv_name": f"{author['lastname']}, {author['firstname']}",
                        "departments": author["departments"],
                        "user_id": user_data["user_id"],
                        "db_name": f"{user_data['lastname']}, {user_data['firstname']}",
                        "pub_count": user_data["pub_count"],
                    }
                )

        # Display results
        print("=" * 80)
        print("AUTHORS FROM CSV WHO HAVE PUBLICATIONS IN DATABASE")
        print("=" * 80)
        print()

        if authors_with_pubs:
            # Sort by publication count (descending)
            authors_with_pubs.sort(key=operator.itemgetter("pub_count"), reverse=True)

            print(f"{'Name':<30} {'User ID':<10} {'Pubs':<6} {'Department':<30}")
            print("-" * 80)

            for author in authors_with_pubs:
                name = author["csv_name"][:29]
                user_id = author["user_id"][:9]
                pub_count = author["pub_count"]
                dept = author["departments"][:29]

                print(f"{name:<30} {user_id:<10} {pub_count:<6} {dept:<30}")

            print()
            print("-" * 80)
            print(
                f"TOTAL: {len(authors_with_pubs)} authors have publications in the database"
            )
            print(f"       Out of {len(csv_authors)} authors listed in CSV")
            print(
                f"       ({len(authors_with_pubs) / len(csv_authors) * 100:.1f}% match rate)"
            )

            # Summary statistics
            total_pubs = sum(a["pub_count"] for a in authors_with_pubs)
            avg_pubs = total_pubs / len(authors_with_pubs)
            print()
            print(f"Total publications: {total_pubs}")
            print(f"Average publications per author: {avg_pubs:.1f}")

        else:
            print(
                "No authors from the CSV were found with publications in the database."
            )

        print("=" * 80)

        return authors_with_pubs

    finally:
        session.close()


def main():
    csv_path = "profiles_without_publications.csv"

    # Check if CSV exists
    if not Path(csv_path).exists():
        print(f"Error: {csv_path} not found!")
        print(f"Current directory: {Path.cwd()}")
        return

    authors_with_pubs = check_authors_with_publications(csv_path)

    # Optional: Save results to a file
    if authors_with_pubs:
        output_file = "authors_with_pubs_found.csv"
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "csv_name",
                    "departments",
                    "user_id",
                    "db_name",
                    "pub_count",
                ],
            )
            writer.writeheader()
            writer.writerows(authors_with_pubs)

        print(f"\nResults saved to: {output_file}")


if __name__ == "__main__":
    main()
