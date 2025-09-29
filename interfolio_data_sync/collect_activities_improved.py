#!/usr/bin/env python3
"""
Improved script to sync publications and grants for all users from Interfolio API
- Adds new records
- Updates existing records  
- Deletes stale records not in API
Publications are in /activities/-21
Grants are in /activities/-11

# Full sync with default settings
python collect_activities_improved.py

# Test with first 100 users
python collect_activities_improved.py --batch 100 --verbose

# Use fewer workers if rate limited
python collect_activities_improved.py --workers 8
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
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from models import Grant, Publication, User
from activity_utils import (
    ActivityTracker,
    create_publication_from_api,
    create_grant_from_api,
    delete_stale_activities,
)


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
        load_dotenv()
        self.database_url = os.getenv("DATABASE_URL")
        self.engine = create_engine(self.database_url, pool_size=20, max_overflow=30)
        self.session_factory = sessionmaker(bind=self.engine)
        self.api = InterfolioAPI()
        self.verbose = verbose
        self.tracker = ActivityTracker()
        self.stats_lock = Lock()
        self.stats = {
            "users_processed": 0,
            "users_with_publications": 0,
            "users_with_grants": 0,
            "publications_added": 0,
            "publications_updated": 0,
            "publications_deleted": 0,
            "grants_added": 0,
            "grants_updated": 0,
            "grants_deleted": 0,
            "duplicates_skipped": 0,
            "parse_errors": 0,
            "db_errors": 0,
        }

    def process_user(self, user_id: str) -> None:
        """Process a single user's publications and grants"""
        session = self.session_factory()
        try:
            # Track this user as seen
            self.tracker.track_user(user_id)
            
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
            publications_updated = 0
            grants_added = 0
            grants_updated = 0
            duplicates = 0

            # Process publications
            for activity in publications:
                try:
                    publication = create_publication_from_api(activity, user_id)
                    if publication and publication.activityid:
                        # Track this publication as seen
                        self.tracker.track_publication(publication.activityid)
                        
                        # Check if already exists
                        existing = (
                            session.query(Publication)
                            .filter_by(activityid=publication.activityid)
                            .first()
                        )

                        if not existing:
                            session.add(publication)
                            publications_added += 1
                            if self.verbose:
                                logging.info(f"  Added publication: {publication.title[:50] if publication.title else 'Untitled'}")
                        else:
                            # Update existing record using merge
                            session.merge(publication)
                            publications_updated += 1
                            if self.verbose:
                                logging.info(f"  Updated publication: {publication.title[:50] if publication.title else 'Untitled'}")

                except IntegrityError:
                    session.rollback()
                    duplicates += 1
                    continue
                except Exception as e:
                    if self.verbose:
                        logging.error(f"  Error processing publication: {e}")
                    with self.stats_lock:
                        self.stats["parse_errors"] += 1
                    continue

            # Process grants
            for activity in grants:
                try:
                    grant = create_grant_from_api(activity, user_id)
                    if grant and grant.activityid:
                        # Track this grant as seen
                        self.tracker.track_grant(grant.activityid)
                        
                        # Check if already exists
                        existing = (
                            session.query(Grant)
                            .filter_by(activityid=grant.activityid)
                            .first()
                        )

                        if not existing:
                            session.add(grant)
                            grants_added += 1
                            if self.verbose:
                                logging.info(f"  Added grant: {grant.title[:50] if grant.title else 'Untitled'}")
                        else:
                            # Update existing record using merge
                            session.merge(grant)
                            grants_updated += 1
                            if self.verbose:
                                logging.info(f"  Updated grant: {grant.title[:50] if grant.title else 'Untitled'}")

                except IntegrityError:
                    session.rollback()
                    duplicates += 1
                    continue
                except Exception as e:
                    if self.verbose:
                        logging.error(f"  Error processing grant: {e}")
                    with self.stats_lock:
                        self.stats["parse_errors"] += 1
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
                self.stats["publications_updated"] += publications_updated
                self.stats["grants_added"] += grants_added
                self.stats["grants_updated"] += grants_updated
                self.stats["duplicates_skipped"] += duplicates

        except Exception as e:
            session.rollback()
            with self.stats_lock:
                self.stats["users_processed"] += 1
                self.stats["db_errors"] += 1
            logging.error(f"Error processing user {user_id}: {e}")
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

    def delete_stale_data(self):
        """Delete records that weren't seen in the API responses."""
        # Safety check: Don't delete if we didn't track any activities
        if not self.tracker.seen_publications and not self.tracker.seen_grants:
            logging.warning("No publications or grants were tracked during sync.")
            logging.warning("Skipping deletion to prevent data loss.")
            return
            
        session = self.session_factory()
        try:
            logging.info("Deleting stale records...")
            
            # Delete stale publications
            publications_deleted = 0
            if self.tracker.seen_publications:
                publications_deleted = delete_stale_activities(
                    session, 
                    Publication, 
                    'activityid', 
                    self.tracker.seen_publications
                )
            
            # Delete stale grants
            grants_deleted = 0
            if self.tracker.seen_grants:
                grants_deleted = delete_stale_activities(
                    session, 
                    Grant, 
                    'activityid', 
                    self.tracker.seen_grants
                )
            
            session.commit()
            
            # Update stats
            self.stats["publications_deleted"] = publications_deleted
            self.stats["grants_deleted"] = grants_deleted
            
            logging.info(f"Deleted: {publications_deleted} publications, {grants_deleted} grants")
            
        except Exception as e:
            session.rollback()
            logging.error(f"Error deleting stale records: {e}")
        finally:
            session.close()

    def collect_activities(self, max_workers: int = 12, batch_size: int = None):
        """Collect activities for all users"""
        logging.info("Starting data synchronization...")

        user_ids = self.get_user_ids()

        if not user_ids:
            logging.error("No users found or error fetching users")
            return

        # If batch_size is specified, process only that many users
        if batch_size:
            user_ids = user_ids[:batch_size]
            logging.info(f"Processing {batch_size} users (batch mode)...")
        else:
            logging.info(f"Processing {len(user_ids)} users...")

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
                            logging.info(f"\nProgress: {i}/{len(user_ids)} users ({rate:.1f} users/sec)")
                            logging.info(f"  Users with publications: {self.stats['users_with_publications']}")
                            logging.info(f"  Users with grants: {self.stats['users_with_grants']}")
                            logging.info(f"  Publications: +{self.stats['publications_added']} new, ~{self.stats['publications_updated']} updated")
                            logging.info(f"  Grants: +{self.stats['grants_added']} new, ~{self.stats['grants_updated']} updated")
                            logging.info(f"  Errors: {self.stats['db_errors'] + self.stats['parse_errors']}")
                except Exception as e:
                    logging.error(f"Task failed: {e}")

        # After processing all users, delete stale records
        self.delete_stale_data()

        elapsed_time = time.time() - start_time

        logging.info("\n" + "=" * 60)
        logging.info("✅ Data synchronization completed!")
        logging.info(f"Time taken: {elapsed_time:.1f} seconds")
        logging.info("Final statistics:")
        logging.info(f"  Users processed: {self.stats['users_processed']}")
        logging.info(f"  Users with publications: {self.stats['users_with_publications']}")
        logging.info(f"  Users with grants: {self.stats['users_with_grants']}")
        logging.info(f"  Publications: +{self.stats['publications_added']} added, ~{self.stats['publications_updated']} updated, -{self.stats['publications_deleted']} deleted")
        logging.info(f"  Grants: +{self.stats['grants_added']} added, ~{self.stats['grants_updated']} updated, -{self.stats['grants_deleted']} deleted")
        logging.info(f"  Duplicates skipped: {self.stats['duplicates_skipped']}")
        logging.info(f"  Parse errors: {self.stats['parse_errors']}")
        logging.info(f"  Database errors: {self.stats['db_errors']}")
        if elapsed_time > 0:
            logging.info(f"  Processing rate: {len(user_ids)/elapsed_time:.1f} users/sec")


def main():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('collect_activities.log'),
            logging.StreamHandler()
        ]
    )

    parser = argparse.ArgumentParser(description="Sync publications and grants for users (add, update, delete)")
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
    parser.add_argument("--verbose", action="store_true", help="Show detailed debug output")

    args = parser.parse_args()

    collector = ActivityCollector(verbose=args.verbose)
    collector.collect_activities(max_workers=args.workers, batch_size=args.batch)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("\n🔚 The End.")
