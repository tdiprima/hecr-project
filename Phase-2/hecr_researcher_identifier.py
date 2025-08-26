import logging
from datetime import datetime
from typing import Dict, List, Set

import psycopg2
from psycopg2.extras import RealDictCursor

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class HECRUserIdentifier:
    def __init__(self, db_config: Dict[str, str]):
        """
        Initialize the HECR user identifier with database configuration.

        Args:
            db_config: Dictionary with keys: host, database, user, password, port (optional)
        """
        self.db_config = db_config
        self.conn = None
        self.cursor = None

    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")

    def create_hecr_table(self, drop_existing: bool = False):
        """
        Create the HECR table with the same structure as users table.

        Args:
            drop_existing: If True, drop existing HECR table first
        """
        try:
            if drop_existing:
                self.cursor.execute("DROP TABLE IF EXISTS hecr CASCADE")
                logger.info("Dropped existing HECR table")

            # Get the structure of users table
            self.cursor.execute(
                """
                SELECT column_name, data_type, character_maximum_length, 
                       is_nullable, column_default
                FROM information_schema.columns
                WHERE table_name = 'users'
                ORDER BY ordinal_position
            """
            )
            columns = self.cursor.fetchall()

            # Build CREATE TABLE statement
            create_stmt = "CREATE TABLE IF NOT EXISTS hecr ("
            col_definitions = []

            for col in columns:
                col_def = f"{col['column_name']} {col['data_type']}"
                if col["character_maximum_length"]:
                    col_def += f"({col['character_maximum_length']})"
                if col["is_nullable"] == "NO":
                    col_def += " NOT NULL"
                if col["column_default"]:
                    col_def += f" DEFAULT {col['column_default']}"
                col_definitions.append(col_def)

            # Add tracking columns
            col_definitions.extend(
                [
                    "identified_via TEXT",
                    "keywords_matched TEXT[]",
                    "date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                ]
            )

            create_stmt += ", ".join(col_definitions) + ")"

            self.cursor.execute(create_stmt)

            # Add primary key constraint if id exists
            self.cursor.execute(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'hecr_pkey') THEN
                        ALTER TABLE hecr ADD CONSTRAINT hecr_pkey PRIMARY KEY (id);
                    END IF;
                END$$;
            """
            )

            self.conn.commit()
            logger.info("HECR table created successfully")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to create HECR table: {e}")
            raise

    def find_users_by_keywords(
        self,
        keyword_sets: Dict[str, List[str]],
        search_publications: bool = True,
        search_grants: bool = True,
    ) -> Set[int]:
        """
        Find users based on keyword matches in publications and/or grants.

        Args:
            keyword_sets: Dictionary of category names to lists of keywords
            search_publications: Whether to search in publications table
            search_grants: Whether to search in grants table

        Returns:
            Set of user IDs that match any keywords
        """
        matched_users = set()

        for category, keywords in keyword_sets.items():
            logger.info(f"Searching for {category} keywords...")

            # Convert keywords to PostgreSQL LIKE patterns
            patterns = [f"%{kw.lower()}%" for kw in keywords]

            if search_publications:
                query = """
                    SELECT DISTINCT p.user_id
                    FROM publications p
                    WHERE LOWER(p.title) LIKE ANY(%s)
                """
                self.cursor.execute(query, (patterns,))
                pub_users = {row["user_id"] for row in self.cursor.fetchall()}
                matched_users.update(pub_users)
                logger.info(
                    f"Found {len(pub_users)} users from publications for {category}"
                )

            if search_grants:
                query = """
                    SELECT DISTINCT g.user_id
                    FROM grants g
                    WHERE LOWER(g.title) LIKE ANY(%s)
                """
                self.cursor.execute(query, (patterns,))
                grant_users = {row["user_id"] for row in self.cursor.fetchall()}
                matched_users.update(grant_users)
                logger.info(
                    f"Found {len(grant_users)} users from grants for {category}"
                )

        logger.info(f"Total unique users found: {len(matched_users)}")
        return matched_users

    def populate_hecr_table(
        self, user_ids: Set[int], identified_via: str = "keyword_search"
    ):
        """
        Populate HECR table with identified users, avoiding duplicates.

        Args:
            user_ids: Set of user IDs to add to HECR table
            identified_via: String to track how users were identified
        """
        if not user_ids:
            logger.info("No users to add")
            return

        try:
            # Get column names from users table (excluding our tracking columns)
            self.cursor.execute(
                """
                SELECT column_name 
                FROM information_schema.columns
                WHERE table_name = 'users'
                ORDER BY ordinal_position
            """
            )
            user_columns = [row["column_name"] for row in self.cursor.fetchall()]

            # Insert users, avoiding duplicates
            insert_query = f"""
                INSERT INTO hecr ({', '.join(user_columns)}, identified_via, date_added)
                SELECT {', '.join(user_columns)}, %s, %s
                FROM users
                WHERE id = ANY(%s)
                ON CONFLICT (id) DO NOTHING
            """

            self.cursor.execute(
                insert_query, (identified_via, datetime.now(), list(user_ids))
            )
            inserted = self.cursor.rowcount

            self.conn.commit()
            logger.info(f"Added {inserted} new users to HECR table")

            # Get total count
            self.cursor.execute("SELECT COUNT(*) as total FROM hecr")
            total = self.cursor.fetchone()["total"]
            logger.info(f"Total users in HECR table: {total}")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to populate HECR table: {e}")
            raise

    def get_hecr_summary(self) -> Dict:
        """Get summary statistics of HECR table"""
        try:
            # Basic count
            self.cursor.execute("SELECT COUNT(*) as total FROM hecr")
            total = self.cursor.fetchone()["total"]

            # Count by identification method
            self.cursor.execute(
                """
                SELECT identified_via, COUNT(*) as count
                FROM hecr
                GROUP BY identified_via
            """
            )
            by_method = self.cursor.fetchall()

            # Sample of users with most matches - Fixed query
            self.cursor.execute(
                """
                SELECT h.id, h.firstname, h.lastname, 
                       COUNT(DISTINCT p.id) as publication_matches,
                       COUNT(DISTINCT g.id) as grant_matches
                FROM hecr h
                LEFT JOIN publications p ON h.id = p.user_id
                LEFT JOIN grants g ON h.id = g.user_id
                GROUP BY h.id, h.firstname, h.lastname
                ORDER BY COUNT(DISTINCT p.id) + COUNT(DISTINCT g.id) DESC
                LIMIT 10
            """
            )
            top_users = self.cursor.fetchall()

            return {
                "total_users": total,
                "by_identification_method": by_method,
                "top_users": top_users,
            }

        except Exception as e:
            logger.error(f"Failed to get summary: {e}")
            raise


def main():
    # Database configuration
    db_config = {
        "host": "localhost",
        "database": "research",
        "user": "your_username",
        "password": "your_password",
        "port": 5432,  # optional, defaults to 5432
    }

    # Define keyword sets - you can easily modify or add more runs
    health_equity_keywords = [
        "health equity",
        "health disparit",
        "healthcare access",
        "healthcare inequ",
        "racial disparit",
        "ethnic disparit",
        "minority health",
        "underserved population",
        "vulnerable population",
        "social determinant",
        "health inequalit",
        "socioeconomic",
        "rural health",
        "urban health",
        "low-income",
    ]

    climate_health_keywords = [
        "climate change health",
        "climate medicine",
        "climate health",
        "planetary health",
        "environmental health",
        "heat wave",
        "heat stress",
        "thermal stress",
        "extreme weather",
        "air pollution",
        "vector-borne disease",
        "disease migration",
        "climate-sensitive disease",
        "zoonotic disease",
    ]

    # Initialize identifier
    identifier = HECRUserIdentifier(db_config)

    try:
        # Connect to database
        identifier.connect()

        # Create HECR table (set drop_existing=True to start fresh)
        identifier.create_hecr_table(drop_existing=False)

        # First run: Health Equity keywords
        logger.info("=== FIRST RUN: Health Equity Keywords ===")
        keyword_sets_run1 = {"Health Equity": health_equity_keywords}
        users_run1 = identifier.find_users_by_keywords(keyword_sets_run1)
        identifier.populate_hecr_table(users_run1, identified_via="health_equity_scan")

        # Second run: Climate Health keywords
        logger.info("=== SECOND RUN: Climate Health Keywords ===")
        keyword_sets_run2 = {"Climate Health": climate_health_keywords}
        users_run2 = identifier.find_users_by_keywords(keyword_sets_run2)
        identifier.populate_hecr_table(users_run2, identified_via="climate_health_scan")

        # Optional: Combined run with refined keywords
        logger.info("=== THIRD RUN: Combined/Refined Keywords ===")
        refined_keywords = {
            "Health-Climate Intersection": [
                "climate justice",
                "environmental justice",
                "climate equity",
                "climate vulnerable",
                "heat island effect health",
                "pollution disparit",
            ]
        }
        users_run3 = identifier.find_users_by_keywords(refined_keywords)
        identifier.populate_hecr_table(users_run3, identified_via="intersection_scan")

        # Get and display summary
        summary = identifier.get_hecr_summary()
        logger.info("=== FINAL SUMMARY ===")
        logger.info(f"Total users in HECR table: {summary['total_users']}")
        logger.info("Users by identification method:")
        for method in summary["by_identification_method"]:
            logger.info(f"  - {method['identified_via']}: {method['count']} users")

        logger.info("\nTop 10 users by publication/grant matches:")
        for user in summary["top_users"]:
            logger.info(
                f"  - {user['firstname']} {user['lastname']} (ID: {user['id']}): "
                f"{user['publication_matches']} publications, "
                f"{user['grant_matches']} grants"
            )

    except Exception as e:
        logger.error(f"Script failed: {e}")

    finally:
        identifier.close()


if __name__ == "__main__":
    main()
