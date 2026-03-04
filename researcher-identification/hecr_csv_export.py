import csv
import os

import psycopg2
from dotenv import load_dotenv
from loguru import logger
from psycopg2.extras import RealDictCursor

load_dotenv()

logger.add("automation.log", rotation="1 MB")


def export_hecr(db_config: dict, output_file: str = "hecr_export.csv"):
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
    db_config = {
        "host": "localhost",
        "database": "research",
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS"),
        "port": 5432,
    }
    export_hecr(db_config, "hecr_export.csv")
