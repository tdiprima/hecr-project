#!/usr/bin/env python3
"""
Script to find where grants are stored in the API
"""
import base64
import datetime
import hashlib
import hmac
import json
import os
from typing import Dict, Optional

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import User


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

        print(f"\nTrying: {url}")

        try:
            response = requests.get(url, headers=headers, timeout=30)
            print(f"  Status: {response.status_code}")

            response.raise_for_status()

            if response.text.strip().startswith("<"):
                print(f"  HTML response (error)")
                return None

            data = response.json()
            return data

        except Exception as e:
            print(f"  Error: {e}")
            return None


def find_grants():
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")

    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Get Joel Saltz
        user = session.query(User).filter(User.lastname == "Saltz").first()
        if not user:
            print("Joel Saltz not found!")
            return

        print("=" * 60)
        print(f"SEARCHING FOR GRANTS FOR: {user.firstname} {user.lastname}")
        print(f"User ID: {user.id}")
        print("=" * 60)

        api = InterfolioAPI()

        # Try different activity endpoints
        endpoints_to_try = [
            # Different activity IDs (grants might be under a different ID)
            ("/activities/-1", f"?data=detailed&userlist={user.id}"),
            ("/activities/-2", f"?data=detailed&userlist={user.id}"),
            ("/activities/-3", f"?data=detailed&userlist={user.id}"),
            ("/activities/-4", f"?data=detailed&userlist={user.id}"),
            ("/activities/-5", f"?data=detailed&userlist={user.id}"),
            ("/activities/-10", f"?data=detailed&userlist={user.id}"),
            ("/activities/-11", f"?data=detailed&userlist={user.id}"),
            ("/activities/-20", f"?data=detailed&userlist={user.id}"),
            ("/activities/-22", f"?data=detailed&userlist={user.id}"),
            ("/activities/-23", f"?data=detailed&userlist={user.id}"),
            ("/activities/-24", f"?data=detailed&userlist={user.id}"),
            ("/activities/-25", f"?data=detailed&userlist={user.id}"),
            # Try positive IDs too
            ("/activities/1", f"?data=detailed&userlist={user.id}"),
            ("/activities/2", f"?data=detailed&userlist={user.id}"),
            ("/activities/3", f"?data=detailed&userlist={user.id}"),
            # Try the general activities endpoint
            ("/activities", f"?data=detailed&userlist={user.id}"),
        ]

        grants_found = {}

        for endpoint, params in endpoints_to_try:
            response = api._make_request(endpoint, params)

            if response and isinstance(response, dict):
                # Check each key in the response
                for key, value in response.items():
                    if isinstance(value, list) and len(value) > 0:
                        print(f"  Found {len(value)} items in key '{key}'")

                        # Check first few items for grant fields
                        for item in value[:3]:
                            if isinstance(item, dict) and "fields" in item:
                                fields = item["fields"]

                                # Check if this looks like a grant
                                if (
                                    "Grant ID / Contract ID" in fields
                                    or "Sponsor" in fields
                                ):
                                    if key not in grants_found:
                                        grants_found[key] = []
                                    grants_found[key].append(item)

                                    print(f"    ✓ GRANT FOUND in key '{key}'!")
                                    print(
                                        f"      Title: {fields.get('Title', 'N/A')[:60]}"
                                    )
                                    print(
                                        f"      Grant ID: {fields.get('Grant ID / Contract ID', 'N/A')}"
                                    )
                                    print(
                                        f"      Sponsor: {fields.get('Sponsor', 'N/A')}"
                                    )

                                # Also show what types are in this endpoint
                                if (
                                    "Type" in fields and item == value[0]
                                ):  # Only for first item
                                    print(
                                        f"    First item type in '{key}': {fields['Type']}"
                                    )

        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)

        if grants_found:
            print(f"Grants found in the following endpoints/keys:")
            for key, grants in grants_found.items():
                print(f"  Key '{key}': {len(grants)} grants")

            # Show full structure of first grant
            first_key = list(grants_found.keys())[0]
            first_grant = grants_found[first_key][0]
            print(f"\nFull structure of first grant:")
            print(json.dumps(first_grant, indent=2)[:2000])
        else:
            print("No grants found in any of the tested endpoints")
            print("\nPossible reasons:")
            print("1. Grants might be in a different endpoint entirely")
            print("2. Joel Saltz might not have any grants in the system")
            print("3. Grants might require different query parameters")

    finally:
        session.close()


if __name__ == "__main__":
    find_grants()
