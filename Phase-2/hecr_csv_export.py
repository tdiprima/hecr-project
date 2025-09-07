import csv

import psycopg2
from loguru import logger
from psycopg2.extras import RealDictCursor

logger.add("automation.log", rotation="1 MB")


def export_hecr_to_csv(
    self,
    output_file: str = "hecr_export.csv",
    include_grants: bool = True,
    include_publications: bool = True,
):
    """
    Export HECR users with their publications and grants to CSV.

    Args:
        output_file: Path to output CSV file
        include_grants: Whether to include grants data
        include_publications: Whether to include publications data
    """
    try:
        # Build the query based on what to include
        select_fields = ["h.firstname", "h.lastname", "h.id as user_id"]
        from_joins = ["hecr h"]

        if include_publications:
            select_fields.extend(
                ["p.title as publication_title", "p.id as publication_id"]
            )
            from_joins.append("LEFT JOIN publications p ON h.id = p.user_id")

        if include_grants:
            select_fields.extend(["g.title as grant_title", "g.id as grant_id"])
            from_joins.append("LEFT JOIN grants g ON h.id = g.user_id")

        # Only include rows where at least one publication or grant exists
        where_clause = ""
        if include_publications and include_grants:
            where_clause = "WHERE p.id IS NOT NULL OR g.id IS NOT NULL"
        elif include_publications:
            where_clause = "WHERE p.id IS NOT NULL"
        elif include_grants:
            where_clause = "WHERE g.id IS NOT NULL"

        query = f"""
            SELECT {', '.join(select_fields)}
            FROM {' '.join(from_joins)}
            {where_clause}
            ORDER BY h.lastname, h.firstname, p.title, g.title
        """

        self.cursor.execute(query)

        # Write to CSV
        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)

            # Write header
            header = ["firstname", "lastname", "user_id"]
            if include_publications:
                header.extend(["publication_title", "publication_id"])
            if include_grants:
                header.extend(["grant_title", "grant_id"])
            writer.writerow(header)

            # Write data
            row_count = 0
            for row in self.cursor.fetchall():
                writer.writerow(
                    [
                        row["firstname"],
                        row["lastname"],
                        row["user_id"],
                        *(
                            [row["publication_title"], row["publication_id"]]
                            if include_publications
                            else []
                        ),
                        *(
                            [row["grant_title"], row["grant_id"]]
                            if include_grants
                            else []
                        ),
                    ]
                )
                row_count += 1

        logger.info(f"Exported {row_count} rows to {output_file}")

        # Also create a summary CSV with user counts
        summary_file = output_file.replace(".csv", "_summary.csv")
        self.cursor.execute(
            """
            SELECT h.firstname, h.lastname, h.id,
                   COUNT(DISTINCT p.id) as publication_count,
                   COUNT(DISTINCT g.id) as grant_count
            FROM hecr h
            LEFT JOIN publications p ON h.id = p.user_id
            LEFT JOIN grants g ON h.id = g.user_id
            GROUP BY h.id, h.firstname, h.lastname
            ORDER BY h.lastname, h.firstname
        """
        )

        with open(summary_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(
                [
                    "firstname",
                    "lastname",
                    "user_id",
                    "publication_count",
                    "grant_count",
                    "total_count",
                ]
            )

            for row in self.cursor.fetchall():
                total = row["publication_count"] + row["grant_count"]
                writer.writerow(
                    [
                        row["firstname"],
                        row["lastname"],
                        row["id"],
                        row["publication_count"],
                        row["grant_count"],
                        total,
                    ]
                )

        logger.info(f"Exported user summary to {summary_file}")

    except Exception as e:
        logger.error(f"Failed to export to CSV: {e}")
        raise


# Alternative: If you want a standalone function (not part of the class)
def export_hecr_standalone(db_config: dict, output_file: str = "hecr_export.csv"):
    """Standalone function to export HECR data to CSV"""

    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Get all data in one query
        query = """
            SELECT h.firstname, h.lastname, h.id as user_id,
                   p.title as publication_title, p.id as publication_id,
                   g.title as grant_title, g.id as grant_id
            FROM hecr h
            LEFT JOIN publications p ON h.id = p.user_id
            LEFT JOIN grants g ON h.id = g.user_id
            WHERE p.id IS NOT NULL OR g.id IS NOT NULL
            ORDER BY h.lastname, h.firstname, p.title, g.title
        """

        cursor.execute(query)

        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(
                [
                    "firstname",
                    "lastname",
                    "user_id",
                    "publication_title",
                    "publication_id",
                    "grant_title",
                    "grant_id",
                ]
            )

            for row in cursor.fetchall():
                writer.writerow(
                    [
                        row["firstname"],
                        row["lastname"],
                        row["user_id"],
                        row["publication_title"],
                        row["publication_id"],
                        row["grant_title"],
                        row["grant_id"],
                    ]
                )

        print(f"Export completed: {output_file}")

    finally:
        if conn:
            conn.close()


# Usage examples:
if __name__ == "__main__":
    # Method 1: Add to your existing class
    # identifier.export_hecr_to_csv("hecr_detailed_export.csv")

    # Method 2: Standalone function
    db_config = {
        "host": "localhost",
        "database": "research",
        "user": "your_username",
        "password": "your_password",
        "port": 5432,
    }
    # export_hecr_standalone(db_config, "hecr_export.csv")
