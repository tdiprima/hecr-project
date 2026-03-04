#!/usr/bin/env python3
"""
Script to collect publications and grants for all users from Interfolio API
Publications are in /activities/-21
Grants are in /activities/-11
"""
import argparse
import base64
import datetime
import hashlib
import hmac
import logging
import os
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Dict, List, Optional

import requests
from dotenv import load_dotenv
from halo import Halo
from models import Grant, Publication, User
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker


class InterfolioAPI:
    def __init__(self):
        self.api_key = os.getenv("API_PUBLIC_KEY")
        self.api_secret = os.getenv("API_PRIVATE_KEY")
        self.database_id = os.getenv("TENANT_1_DATABASE_ID")
        self.api_host = "https://faculty180.interfolio.com/api.php"

        if not all([self.api_key, self.api_secret, self.database_id]):
            raise ValueError("Missing required API credentials in .env file")

    def _build_message(self, method: str, timestamp: str, request_string: str) -> str:
        return f"{method}\n\n\n{timestamp}\n{request_string}"

    def _generate_signature(self, message: str) -> str:
        signature_bytes = hmac.new(
            self.api_secret.encode(), message.encode(), hashlib.sha1
        ).digest()
        return base64.b64encode(signature_bytes).decode()

    def _build_auth_header(self, signature: str) -> str:
        return f"INTF {self.api_key}:{signature}"

    def _make_request(self, endpoint: str, query_params: str = "") -> Optional[Dict]:
        timestamp = datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        message = self._build_message("GET", timestamp, endpoint)
        signature = self._generate_signature(message)
        auth_header = self._build_auth_header(signature)

        url = f"{self.api_host}{endpoint}{query_params}"
        headers = {
            "TimeStamp": timestamp,
            "Authorization": auth_header,
            "INTF-DatabaseID": self.database_id,
        }

        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=headers, timeout=30)

                if response.status_code == 429:  # Rate limited
                    wait_time = (attempt + 1) * 2
                    time.sleep(wait_time)
                    continue

                response.raise_for_status()

                if response.text.strip().startswith("<"):
                    # HTML error response
                    return None

                return response.json()
            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                return None
            except Exception:
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
                return None

        return None

    def get_user_publications(self, user_id: str) -> List[Dict]:
        """Get user publications from /activities/-21 endpoint"""
        response = self._make_request(
            "/activities/-21", f"?data=detailed&userlist={user_id}"
        )

        if not response or not isinstance(response, dict):
            return []

        # Publications are in the '-21' key
        if "-21" in response and isinstance(response["-21"], list):
            return response["-21"]

        return []

    def get_user_grants(self, user_id: str) -> List[Dict]:
        """Get user grants from /activities/-11 endpoint"""
        response = self._make_request(
            "/activities/-11", f"?data=detailed&userlist={user_id}"
        )

        if not response or not isinstance(response, dict):
            return []

        # Grants are in the '-11' key
        if "-11" in response and isinstance(response["-11"], list):
            return response["-11"]

        return []


class ActivityCollector:
    def __init__(self, verbose=False):
        self.database_url = os.getenv("DATABASE_URL")
        self.engine = create_engine(self.database_url, pool_size=20, max_overflow=30)
        self.session_factory = sessionmaker(bind=self.engine)
        self.api = InterfolioAPI()
        self.verbose = verbose
        self.stats_lock = Lock()
        self.stats = {
            "users_processed": 0,
            "users_with_publications": 0,
            "users_with_grants": 0,
            "publications_added": 0,
            "grants_added": 0,
            "duplicates_skipped": 0,
            "parse_errors": 0,
            "db_errors": 0,
        }

    def _truncate_field(self, value: Optional[str], max_length: int) -> Optional[str]:
        """Truncate a field to max_length if it's too long"""
        if value and len(str(value)) > max_length:
            return str(value)[:max_length]
        return value

    def _create_publication(
        self, activity_data: Dict, user_id: str
    ) -> Optional[Publication]:
        """Create a Publication object from activity data"""
        try:
            fields = activity_data.get("fields", {})
            activity_type = fields.get("Type", "")

            # Only process Journal Articles and Books
            if activity_type not in ("Journal Article", "Book"):
                return None

            # Extract status info - handle both list and dict formats
            status_info = {}
            if activity_data.get("status"):
                if (
                    isinstance(activity_data["status"], list)
                    and len(activity_data["status"]) > 0
                ):
                    status_info = activity_data["status"][0]
                elif isinstance(activity_data["status"], dict):
                    status_info = activity_data["status"]

            # Parse year - handle various formats
            year = fields.get("Year")
            if year:
                year = str(year)[:4] if len(str(year)) >= 4 else str(year)

            # Truncate fields to match database constraints
            # Most varchar fields are 50 or 255 characters
            return Publication(
                user_id=user_id,
                activityid=activity_data.get("activityid"),
                type=self._truncate_field(activity_type, 50),
                title=self._truncate_field(fields.get("Title"), 255),
                journal=self._truncate_field(fields.get("Journal Title"), 255),
                series_title=self._truncate_field(fields.get("Series Title"), 255),
                year=self._truncate_field(year, 4),
                month_season=self._truncate_field(fields.get("Month / Season"), 50),
                publisher=self._truncate_field(fields.get("Publisher"), 255),
                publisher_city_state=self._truncate_field(
                    fields.get("Publisher City and State"), 255
                ),
                publisher_country=self._truncate_field(
                    fields.get("Publisher Country"), 100
                ),
                volume=self._truncate_field(fields.get("Volume"), 50),
                issue_number=self._truncate_field(
                    fields.get("Issue Number / Edition"), 50
                ),
                page_numbers=self._truncate_field(
                    fields.get("Page Number(s) or Number of Pages"), 50
                ),
                isbn=self._truncate_field(fields.get("ISBN"), 20),
                issn=self._truncate_field(fields.get("ISSN"), 20),
                doi=self._truncate_field(fields.get("DOI"), 255),
                url=self._truncate_field(fields.get("URL"), 500),
                description=fields.get("Description"),
                origin=self._truncate_field(fields.get("Origin"), 50),
                status=self._truncate_field(
                    status_info.get("status") if status_info else None, 50
                ),
                term=self._truncate_field(
                    status_info.get("term") if status_info else None, 50
                ),
                status_year=self._truncate_field(
                    (
                        str(status_info.get("year"))
                        if status_info and status_info.get("year")
                        else None
                    ),
                    4,
                ),
            )
        except Exception as e:
            if self.verbose:
                logging.error(f"Error creating publication: {e}")
            with self.stats_lock:
                self.stats["parse_errors"] += 1
            return None

    def _create_grant(self, activity_data: Dict, user_id: str) -> Optional[Grant]:
        """Create a Grant object from activity data"""
        try:
            fields = activity_data.get("fields", {})

            # All items from -11 endpoint should be grants
            # but double-check for Grant ID
            if not fields.get("Grant ID / Contract ID"):
                return None

            # Extract status info
            status_info = {}
            if activity_data.get("status"):
                if (
                    isinstance(activity_data["status"], list)
                    and len(activity_data["status"]) > 0
                ):
                    status_info = activity_data["status"][0]
                elif isinstance(activity_data["status"], dict):
                    status_info = activity_data["status"]

            # Extract funding info
            funding_info = {}
            if activity_data.get("funding"):
                if isinstance(activity_data["funding"], dict):
                    # The funding dict has the activityid as key
                    activity_id = str(activity_data.get("activityid"))
                    if activity_id in activity_data["funding"]:
                        funding_info = activity_data["funding"][activity_id]
                    else:
                        # Get first funding entry
                        funding_values = list(activity_data["funding"].values())
                        if funding_values and isinstance(funding_values[0], dict):
                            funding_info = funding_values[0]

            # Get total funding from various possible fields
            total_funding = (
                funding_info.get("fundedamount")
                or fields.get("Total Funding")
                or fields.get("Amount")
            )

            # Truncate fields to match database constraints
            return Grant(
                user_id=user_id,
                activityid=activity_data.get("activityid"),
                title=self._truncate_field(fields.get("Title"), 255),
                sponsor=self._truncate_field(fields.get("Sponsor"), 255),
                grant_id=self._truncate_field(
                    fields.get("Grant ID / Contract ID"), 100
                ),
                award_date=fields.get("Award Date"),
                start_date=fields.get("Start Date"),
                end_date=fields.get("End Date"),
                period_length=fields.get("Period Length"),
                period_unit=self._truncate_field(fields.get("Period Unit"), 50),
                indirect_funding=fields.get("Indirect Funding"),
                indirect_cost_rate=self._truncate_field(
                    fields.get("Indirect Cost Rate"), 50
                ),
                total_funding=self._truncate_field(
                    str(total_funding) if total_funding else None, 50
                ),
                total_direct_funding=self._truncate_field(
                    (
                        str(fields.get("Total Direct Funding"))
                        if fields.get("Total Direct Funding")
                        else None
                    ),
                    50,
                ),
                currency_type=self._truncate_field(fields.get("Currency Type"), 10),
                description=fields.get("Description"),
                abstract=fields.get("Abstract"),
                number_of_periods=fields.get("Number of Periods"),
                url=self._truncate_field(fields.get("URL"), 500),
                status=self._truncate_field(
                    status_info.get("status") if status_info else None, 50
                ),
                term=self._truncate_field(
                    status_info.get("term") if status_info else None, 50
                ),
                status_year=self._truncate_field(
                    (
                        str(status_info.get("year"))
                        if status_info and status_info.get("year")
                        else None
                    ),
                    4,
                ),
            )
        except Exception as e:
            if self.verbose:
                logging.error(f"Error creating grant: {e}")
                traceback.logging.info_exc()
            with self.stats_lock:
                self.stats["parse_errors"] += 1
            return None

    def process_user(self, user_id: str) -> None:
        """Process a single user's publications and grants"""
        session = self.session_factory()
        try:
            # Get publications from API
            publications = self.api.get_user_publications(user_id)

            # Get grants from API
            grants = self.api.get_user_grants(user_id)

            if not publications and not grants:
                with self.stats_lock:
                    self.stats["users_processed"] += 1
                if self.verbose:
                    logging.info(f"User {user_id}: No activities found")
                return

            if publications:
                with self.stats_lock:
                    self.stats["users_with_publications"] += 1

            if grants:
                with self.stats_lock:
                    self.stats["users_with_grants"] += 1

            publications_added = 0
            grants_added = 0
            duplicates = 0

            # Process publications
            for activity in publications:
                try:
                    publication = self._create_publication(activity, user_id)
                    if publication and publication.activityid:
                        # Check if already exists
                        existing = (
                            session.query(Publication)
                            .filter(
                                Publication.activityid == publication.activityid,
                                Publication.user_id == user_id,
                            )
                            .first()
                        )

                        if not existing:
                            session.add(publication)
                            publications_added += 1
                            if self.verbose:
                                logging.info(
                                    f"  Added publication: {publication.title[:50] if publication.title else 'Untitled'}"
                                )
                        else:
                            duplicates += 1

                except IntegrityError:
                    session.rollback()
                    duplicates += 1
                    continue
                except Exception as e:
                    if self.verbose:
                        logging.info(f"  Error processing publication: {e}")
                    continue

            # Process grants
            for activity in grants:
                try:
                    grant = self._create_grant(activity, user_id)
                    if grant and grant.activityid:
                        # Check if already exists
                        existing = (
                            session.query(Grant)
                            .filter(
                                Grant.activityid == grant.activityid,
                                Grant.user_id == user_id,
                            )
                            .first()
                        )

                        if not existing:
                            session.add(grant)
                            grants_added += 1
                            if self.verbose:
                                logging.info(
                                    f"  Added grant: {grant.title[:50] if grant.title else 'Untitled'}"
                                )
                        else:
                            duplicates += 1

                except IntegrityError:
                    session.rollback()
                    duplicates += 1
                    continue
                except Exception as e:
                    if self.verbose:
                        logging.info(f"  Error processing grant: {e}")
                    continue

            # Commit all changes for this user
            try:
                session.commit()
            except Exception as e:
                session.rollback()
                with self.stats_lock:
                    self.stats["db_errors"] += 1
                logging.error(f"Error committing data for user {user_id}: {e}")
                return

            # Update statistics
            with self.stats_lock:
                self.stats["users_processed"] += 1
                self.stats["publications_added"] += publications_added
                self.stats["grants_added"] += grants_added
                self.stats["duplicates_skipped"] += duplicates

            # if publications_added > 0 or grants_added > 0:
            #     logging.info(f"User {user_id}: +{publications_added} pubs, +{grants_added} grants")

        except Exception as e:
            session.rollback()
            with self.stats_lock:
                self.stats["users_processed"] += 1
                self.stats["db_errors"] += 1
            logging.info(f"Error processing user {user_id}: {e}")
            if self.verbose:
                traceback.logging.info_exc()
        finally:
            session.close()

    def get_user_ids(self) -> List[str]:
        """Get all non-staff user IDs from database"""
        session = self.session_factory()
        try:
            users = (
                session.query(User.id).filter(User.employmentstatus != "Staff").all()
            )
            return [user.id for user in users]
        finally:
            session.close()

    def collect_activities(self, max_workers: int = 12, batch_size: int = None):
        """Collect activities for all users"""
        spinner = Halo(text="Getting user list...", spinner="line", color="magenta")
        spinner.start()

        user_ids = self.get_user_ids()

        if not user_ids:
            spinner.fail("No users found in database")
            return

        # If batch_size is specified, process only that many users
        if batch_size:
            user_ids = user_ids[:batch_size]
            spinner.text = f"Processing {batch_size} users (batch mode)..."
        else:
            spinner.text = f"Processing {len(user_ids)} users..."

        spinner.succeed(f"Found {len(user_ids)} users to process")
        logging.info(f"Using {max_workers} workers")
        logging.info("Fetching from:")
        logging.info("  Publications: /activities/-21")
        logging.info("  Grants: /activities/-11")

        start_time = time.time()

        # Process users in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self.process_user, user_id) for user_id in user_ids
            ]

            for i, future in enumerate(as_completed(futures), 1):
                try:
                    future.result()
                    if i % 100 == 0 or i == len(user_ids):
                        with self.stats_lock:
                            elapsed = time.time() - start_time
                            rate = i / elapsed if elapsed > 0 else 0
                            logging.info(
                                f"\nProgress: {i}/{len(user_ids)} users ({rate:.1f} users/sec)"
                            )
                            logging.info(
                                f"  Users with publications: {self.stats['users_with_publications']}"
                            )
                            logging.info(
                                f"  Users with grants: {self.stats['users_with_grants']}"
                            )
                            logging.info(
                                f"  Publications added: {self.stats['publications_added']}"
                            )
                            logging.info(
                                f"  Grants added: {self.stats['grants_added']}"
                            )
                            logging.info(
                                f"  Errors: {self.stats['db_errors'] + self.stats['parse_errors']}"
                            )
                except Exception as e:
                    logging.error(f"Task failed: {e}")

        elapsed_time = time.time() - start_time

        logging.info("\n" + "=" * 60)
        logging.info("✅ Data collection completed!")
        logging.info(f"Time taken: {elapsed_time:.1f} seconds")
        logging.info("Final statistics:")
        logging.info(f"  Users processed: {self.stats['users_processed']}")
        logging.info(
            f"  Users with publications: {self.stats['users_with_publications']}"
        )
        logging.info(f"  Users with grants: {self.stats['users_with_grants']}")
        logging.info(f"  Publications added: {self.stats['publications_added']}")
        logging.info(f"  Grants added: {self.stats['grants_added']}")
        logging.info(f"  Duplicates skipped: {self.stats['duplicates_skipped']}")
        logging.info(f"  Parse errors: {self.stats['parse_errors']}")
        logging.info(f"  Database errors: {self.stats['db_errors']}")
        if elapsed_time > 0:
            logging.info(
                f"  Processing rate: {len(user_ids)/elapsed_time:.1f} users/sec"
            )


def main():
    load_dotenv()

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("gather_data.log"), logging.StreamHandler()],
    )

    parser = argparse.ArgumentParser(
        description="Collect publications and grants for users"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=12,
        help="Number of concurrent workers (default: 12)",
    )
    parser.add_argument(
        "--batch",
        type=int,
        default=None,
        help="Process only first N users (for testing)",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Show detailed debug output"
    )

    args = parser.parse_args()

    collector = ActivityCollector(verbose=args.verbose)
    collector.collect_activities(max_workers=args.workers, batch_size=args.batch)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("\n🔚 The End.")
