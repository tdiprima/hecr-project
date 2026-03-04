#!/usr/bin/env python3
"""
Script to update authors_with_pubs_found.csv with departments from profiles_without_publications.csv
for records where db=grace
"""
import csv


def parse_name(name_str):
    """Parse 'Lastname, Firstname' or 'Firstname Lastname' format into tuple"""
    name_str = name_str.strip()

    if "," in name_str:
        # Format: "Lastname, Firstname"
        parts = name_str.split(",")
        lastname = parts[0].strip()
        firstname = parts[1].strip() if len(parts) > 1 else ""
    else:
        # Format: "Firstname Lastname"
        parts = name_str.split()
        if len(parts) >= 2:
            firstname = parts[0].strip()
            lastname = " ".join(parts[1:]).strip()
        else:
            firstname = ""
            lastname = parts[0].strip() if parts else ""

    return (lastname.lower(), firstname.lower())


def load_department_lookup(csv_path):
    """Load departments from profiles_without_publications.csv into a lookup dict"""
    lookup = {}

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            lastname = row["Lastname"].strip().lower()
            firstname = row["Firstname"].strip().lower()
            department = row["Departments"].strip()

            key = (lastname, firstname)
            lookup[key] = department

    return lookup


def update_authors_file(authors_file, profiles_file, output_file):
    """Update authors_with_pubs_found.csv with departments for db=grace records"""

    # Load department lookup
    dept_lookup = load_department_lookup(profiles_file)
    print(f"Loaded {len(dept_lookup)} departments from {profiles_file}")

    # Read and update the authors file
    updated_rows = []
    updates_count = 0
    not_found_count = 0

    with open(authors_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames

        for row in reader:
            db = row.get("db", "").strip()

            # Only update rows where db=grace and departments is empty
            if db == "grace" and not row["departments"].strip():
                # Parse the db_name
                lastname, firstname = parse_name(row["db_name"])
                key = (lastname, firstname)

                # Look up the department
                if key in dept_lookup:
                    row["departments"] = dept_lookup[key]
                    updates_count += 1
                    print(f"✓ Updated: {row['db_name']} -> {dept_lookup[key]}")
                else:
                    not_found_count += 1
                    print(f"✗ Not found: {row['db_name']} (parsed as: {key})")

            updated_rows.append(row)

    # Write the updated file
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(updated_rows)

    print(f"\n{'=' * 60}")
    print("Summary:")
    print(f"  Updated: {updates_count} records")
    print(f"  Not found: {not_found_count} records")
    print(f"  Output written to: {output_file}")
    print("=" * 60)


def main():
    authors_file = "authors_with_pubs_found.csv"
    profiles_file = "profiles_without_publications.csv"
    output_file = "authors_with_pubs_found.csv"  # Overwrite the original

    update_authors_file(authors_file, profiles_file, output_file)


if __name__ == "__main__":
    main()
