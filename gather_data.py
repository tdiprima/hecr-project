#!/usr/bin/env python3
import base64
import datetime
import hashlib
import hmac
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from models import Base, Grant, Publication, User


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

        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()

            if response.text.strip().startswith("<"):
                print(f"HTML response received for {url}")
                return None

            return response.json()
        except Exception as e:
            print(f"Error making request to {url}: {e}")
            return None

    def get_users(self) -> List[Dict]:
        print("Fetching all users...")
        data = self._make_request("/users", "?data=detailed")
        if data and isinstance(data, list):
            print(f"Found {len(data)} users")
            return data
        return []

    def get_user_activities(self, user_id: str) -> List[Dict]:
        data = self._make_request(
            "/activities/-21", f"?data=detailed&userlist={user_id}"
        )
        if data and isinstance(data, list):
            return data
        return []


class DataCollector:
    def __init__(self):
        load_dotenv()
        self.database_url = os.getenv(
            "DATABASE_URL", "postgresql://admin:secret@localhost/research"
        )
        self.engine = create_engine(self.database_url, pool_size=20, max_overflow=30)
        self.session_factory = sessionmaker(bind=self.engine)
        self.api = InterfolioAPI()
        self.stats_lock = Lock()
        self.stats = {
            "users_processed": 0,
            "publications_added": 0,
            "grants_added": 0,
            "errors": 0,
        }

    def _create_user(self, user_data: Dict) -> User:
        return User(
            id=str(user_data.get("userid", "")),
            email=user_data.get("email"),
            firstname=user_data.get("firstname"),
            lastname=user_data.get("lastname"),
            middlename=user_data.get("middlename"),
            employmentstatus=user_data.get("employmentstatus"),
            position=user_data.get("position"),
            primaryunit=user_data.get("primaryunit"),
            orcid=user_data.get("orcid"),
            rank=user_data.get("rank"),
            url=user_data.get("url"),
            lastlogin=user_data.get("lastlogin"),
            pid=user_data.get("pid"),
        )

    def _create_publication(
        self, activity_data: Dict, user_id: str
    ) -> Optional[Publication]:
        fields = activity_data.get("fields", {})
        activity_type = fields.get("Type", "")

        if activity_type not in ["Journal Article", "Book"]:
            return None

        status_info = (
            activity_data.get("status", [{}])[0] if activity_data.get("status") else {}
        )

        return Publication(
            user_id=user_id,
            activityid=activity_data.get("activityid"),
            type=activity_type,
            title=fields.get("Title"),
            journal=fields.get("Journal Title"),
            series_title=fields.get("Series Title"),
            year=fields.get("Year"),
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
            status=status_info.get("status"),
            term=status_info.get("term"),
            status_year=status_info.get("year"),
        )

    def _create_grant(self, activity_data: Dict, user_id: str) -> Optional[Grant]:
        fields = activity_data.get("fields", {})

        if not fields.get("Grant ID / Contract ID"):
            return None

        status_info = (
            activity_data.get("status", [{}])[0] if activity_data.get("status") else {}
        )
        funding_info = (
            list(activity_data.get("funding", {}).values())[0]
            if activity_data.get("funding")
            else {}
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
            total_funding=funding_info.get("fundedamount")
            or fields.get("Total Funding"),
            total_direct_funding=fields.get("Total Direct Funding"),
            currency_type=fields.get("Currency Type"),
            description=fields.get("Description"),
            abstract=fields.get("Abstract"),
            number_of_periods=fields.get("Number of Periods"),
            url=fields.get("URL"),
            status=status_info.get("status"),
            term=status_info.get("term"),
            status_year=status_info.get("year"),
        )

    def process_user(self, user_data: Dict) -> None:
        user_id = str(user_data.get("userid", ""))
        if not user_id:
            return

        session = self.session_factory()
        try:
            existing_user = session.query(User).filter(User.id == user_id).first()
            if not existing_user:
                user = self._create_user(user_data)
                session.merge(user)
                session.commit()

            activities = self.api.get_user_activities(user_id)

            publications_added = 0
            grants_added = 0

            for activity in activities:
                try:
                    publication = self._create_publication(activity, user_id)
                    if publication:
                        session.merge(publication)
                        publications_added += 1

                    grant = self._create_grant(activity, user_id)
                    if grant:
                        session.merge(grant)
                        grants_added += 1

                except IntegrityError:
                    session.rollback()
                    continue

            session.commit()

            with self.stats_lock:
                self.stats["users_processed"] += 1
                self.stats["publications_added"] += publications_added
                self.stats["grants_added"] += grants_added

            print(
                f"Processed user {user_id}: {publications_added} publications, {grants_added} grants"
            )

        except Exception as e:
            session.rollback()
            with self.stats_lock:
                self.stats["errors"] += 1
            print(f"Error processing user {user_id}: {e}")
        finally:
            session.close()

    def collect_data(self, max_workers: int = 16):
        print("Starting data collection...")

        users = self.api.get_users()
        if not users:
            print("No users found or error fetching users")
            return

        print(f"Processing {len(users)} users with {max_workers} workers...")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self.process_user, user) for user in users]

            for i, future in enumerate(as_completed(futures), 1):
                try:
                    future.result()
                    if i % 10 == 0:
                        with self.stats_lock:
                            print(f"Progress: {i}/{len(users)} users processed")
                            print(f"Stats: {self.stats}")
                except Exception as e:
                    print(f"Task failed: {e}")

        print("\nData collection completed!")
        print(f"Final stats: {self.stats}")


def main():
    collector = DataCollector()
    collector.collect_data(max_workers=16)


if __name__ == "__main__":
    main()
