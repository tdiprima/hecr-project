#!/usr/bin/env python3
"""
This script reads user data from a database, then uses the Interfolio API
to retrieve and store their activity data, including publications and grants.
"""
import base64
import datetime
import hashlib
import hmac
import os
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Dict, List, Optional

import requests
from dotenv import load_dotenv
from halo import Halo
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from models import Grant, Publication, User


class InterfolioAPI:
    def __init__(self):
        load_dotenv()
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
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
                print(f"Error on final attempt for {endpoint}: {e}")
                return None

        return None

    def get_user_activities(self, user_id: str) -> List[Dict]:
        """Get user activities using the /activities/-21 endpoint"""
        data = self._make_request(
            "/activities/-21", f"?data=detailed&userlist={user_id}"
        )
        if data and isinstance(data, list):
            return data
        return []


class ActivityCollector:
    def __init__(self, verbose=False):
        load_dotenv()
        self.database_url = os.getenv(
            "DATABASE_URL", "postgresql://admin:secret@localhost/research"
        )
        self.engine = create_engine(self.database_url, pool_size=20, max_overflow=30)
        self.session_factory = sessionmaker(bind=self.engine)
        self.api = InterfolioAPI()
        self.verbose = verbose
        self.stats_lock = Lock()
        self.stats = {
            "users_processed": 0,
            "users_with_activities": 0,
            "publications_added": 0,
            "grants_added": 0,
            "duplicates_skipped": 0,
            "parse_errors": 0,
            "db_errors": 0,
        }

    def _create_publication(
        self, activity_data: Dict, user_id: str
    ) -> Optional[Publication]:
        """Create a Publication object from activity data"""
        try:
            fields = activity_data.get("fields", {})
            activity_type = fields.get("Type", "")

            # Only process Journal Articles and Books
            if activity_type not in ["Journal Article", "Book"]:
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

            return Publication(
                user_id=user_id,
                activityid=activity_data.get("activityid"),
                type=activity_type,
                title=fields.get("Title"),
                journal=fields.get("Journal Title"),
                series_title=fields.get("Series Title"),
                year=year,
                month_season=fields.get("Month / Season"),
                publisher=fields.get("Publisher"),
                publisher_city_state=fields.get("Publisher City and State"),
                publisher_country=fields.get("Publisher Country"),
                volume=fields.get("Volume"),
                issue_number=fields.get("Issue Number / Edition"),
                page_numbers=fields.get("Page Number(s) or Number of Pages"),
                isbn=fields.get("ISBN"),
                issn=fields.get("ISSN"),
                doi=fields.get("DOI"),
                url=fields.get("URL"),
                description=fields.get("Description"),
                origin=fields.get("Origin"),
                status=status_info.get("status") if status_info else None,
                term=status_info.get("term") if status_info else None,
                status_year=status_info.get("year") if status_info else None,
            )
        except Exception as e:
            if self.verbose:
                print(f"Error creating publication: {e}")
            with self.stats_lock:
                self.stats["parse_errors"] += 1
            return None

    def _create_grant(self, activity_data: Dict, user_id: str) -> Optional[Grant]:
        """Create a Grant object from activity data"""
        try:
            fields = activity_data.get("fields", {})

            # Only process if it has a Grant ID
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
                    funding_values = list(activity_data["funding"].values())
                    if funding_values and isinstance(funding_values[0], dict):
                        funding_info = funding_values[0]

            # Get total funding from various possible fields
            total_funding = (
                funding_info.get("fundedamount")
                or fields.get("Total Funding")
                or fields.get("Amount")
            )

            return Grant(
                user_id=user_id,
                activityid=activity_data.get("activityid"),
                title=fields.get("Title"),
                sponsor=fields.get("Sponsor"),
                grant_id=fields.get("Grant ID / Contract ID"),
                award_date=fields.get("Award Date"),
                start_date=fields.get("Start Date"),
                end_date=fields.get("End Date"),
                period_length=fields.get("Period Length"),
                period_unit=fields.get("Period Unit"),
                indirect_funding=fields.get("Indirect Funding"),
                indirect_cost_rate=fields.get("Indirect Cost Rate"),
                total_funding=total_funding,
                total_direct_funding=fields.get("Total Direct Funding"),
                currency_type=fields.get("Currency Type"),
                description=fields.get("Description"),
                abstract=fields.get("Abstract"),
                number_of_periods=fields.get("Number of Periods"),
                url=fields.get("URL"),
                status=status_info.get("status") if status_info else None,
                term=status_info.get("term") if status_info else None,
                status_year=status_info.get("year") if status_info else None,
            )
        except Exception as e:
            if self.verbose:
                print(f"Error creating grant: {e}")
            with self.stats_lock:
                self.stats["parse_errors"] += 1
            return None

    def process_user(self, user_id: str) -> None:
        """Process a single user's activities"""
        session = self.session_factory()
        try:
            # Get user activities from API
            activities = self.api.get_user_activities(user_id)

            if not activities:
                with self.stats_lock:
                    self.stats["users_processed"] += 1
                if self.verbose:
                    print(f"User {user_id}: No activities returned")
                return

            with self.stats_lock:
                self.stats["users_with_activities"] += 1

            publications_added = 0
            grants_added = 0
            duplicates = 0

            for activity in activities:
                try:
                    # Try to create and save publication
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
                                print(
                                    f"  Added publication: {publication.title[:50] if publication.title else 'Untitled'}"
                                )
                        else:
                            duplicates += 1

                    # Try to create and save grant
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
                                print(
                                    f"  Added grant: {grant.title[:50] if grant.title else 'Untitled'}"
                                )
                        else:
                            duplicates += 1

                except IntegrityError as e:
                    session.rollback()
                    duplicates += 1
                    if self.verbose:
                        print(f"  Integrity error (likely duplicate): {e}")
                    continue
                except Exception as e:
                    if self.verbose:
                        print(f"  Error processing activity: {e}")
                        traceback.print_exc()
                    continue

            # Commit all changes for this user
            try:
                session.commit()
            except Exception as e:
                session.rollback()
                with self.stats_lock:
                    self.stats["db_errors"] += 1
                print(f"Error committing data for user {user_id}: {e}")
                return

            # Update statistics
            with self.stats_lock:
                self.stats["users_processed"] += 1
                self.stats["publications_added"] += publications_added
                self.stats["grants_added"] += grants_added
                self.stats["duplicates_skipped"] += duplicates

            if publications_added > 0 or grants_added > 0:
                print(
                    f"User {user_id}: +{publications_added} pubs, +{grants_added} grants"
                )

        except Exception as e:
            session.rollback()
            with self.stats_lock:
                self.stats["users_processed"] += 1
                self.stats["db_errors"] += 1
            print(f"Error processing user {user_id}: {e}")
            if self.verbose:
                traceback.print_exc()
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

    def collect_activities(self, max_workers: int = 8, batch_size: int = None):
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
        print(f"Using {max_workers} workers")

        start_time = time.time()

        # Process users in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self.process_user, user_id) for user_id in user_ids
            ]

            for i, future in enumerate(as_completed(futures), 1):
                try:
                    future.result()
                    if i % 50 == 0 or i == len(user_ids):
                        with self.stats_lock:
                            elapsed = time.time() - start_time
                            rate = i / elapsed if elapsed > 0 else 0
                            print(
                                f"\nProgress: {i}/{len(user_ids)} users ({rate:.1f} users/sec)"
                            )
                            print(
                                f"  Users with activities: {self.stats['users_with_activities']}"
                            )
                            print(
                                f"  Publications added: {self.stats['publications_added']}"
                            )
                            print(f"  Grants added: {self.stats['grants_added']}")
                            print(
                                f"  Errors: {self.stats['db_errors'] + self.stats['parse_errors']}"
                            )
                except Exception as e:
                    print(f"Task failed: {e}")

        elapsed_time = time.time() - start_time

        print("\n" + "=" * 60)
        print("✅ Data collection completed!")
        print(f"Time taken: {elapsed_time:.1f} seconds")
        print(f"Final statistics:")
        print(f"  Users processed: {self.stats['users_processed']}")
        print(f"  Users with activities: {self.stats['users_with_activities']}")
        print(f"  Publications added: {self.stats['publications_added']}")
        print(f"  Grants added: {self.stats['grants_added']}")
        print(f"  Duplicates skipped: {self.stats['duplicates_skipped']}")
        print(f"  Parse errors: {self.stats['parse_errors']}")
        print(f"  Database errors: {self.stats['db_errors']}")
        if elapsed_time > 0:
            print(f"  Processing rate: {len(user_ids)/elapsed_time:.1f} users/sec")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Collect publications and grants for users"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Number of concurrent workers (default: 8)",
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
    main()
