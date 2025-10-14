#!/usr/bin/env python3
"""
Script to add Lastname and Firstname columns to authors_with_pubs_found.csv
by parsing db_name based on the db value (grace vs local)
"""
import csv


def parse_name_by_db(db_name, db):
    """
    Parse name based on database source format:
    - grace: "Firstname Lastname"
    - local: "Lastname, Firstname"

    Returns: (lastname, firstname)
    """
    db_name = db_name.strip()
    db = db.strip()

    if db == "grace":
        # Format: "Firstname Lastname"
        parts = db_name.split()
        if len(parts) >= 2:
            firstname = parts[0]
            lastname = " ".join(parts[1:])
        else:
            firstname = ""
            lastname = db_name
    else:  # local or other
        # Format: "Lastname, Firstname"
        if "," in db_name:
            parts = db_name.split(",")
            lastname = parts[0].strip()
            firstname = parts[1].strip() if len(parts) > 1 else ""
        else:
            # Fallback if no comma found
            lastname = db_name
            firstname = ""

    return (lastname, firstname)


def add_name_columns(input_file, output_file):
    """Add Lastname and Firstname columns to the CSV"""

    rows = []

    with open(input_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            db_name = row["db_name"]
            db = row["db"]

            # Parse the name
            lastname, firstname = parse_name_by_db(db_name, db)

            # Add new columns
            row["Lastname"] = lastname
            row["Firstname"] = firstname

            rows.append(row)
            print(
                f"{db_name:30} ({db:5}) -> Lastname: {lastname:20} Firstname: {firstname}"
            )

    # Write updated file with new column order
    fieldnames = ["Lastname", "Firstname", "db_name", "departments", "db"]

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n{'=' * 60}")
    print(f"Added Lastname and Firstname columns to {len(rows)} rows")
    print(f"Output written to: {output_file}")
    print('=' * 60)


def main():
    input_file = "authors_with_pubs_found.csv"
    output_file = "authors_with_pubs_found.csv"  # Overwrite the original

    add_name_columns(input_file, output_file)


if __name__ == "__main__":
    main()
