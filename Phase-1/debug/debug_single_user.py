#!/usr/bin/env python3
"""
Debug script that correctly handles the nested API response structure
"""
import base64
import datetime
import hashlib
import hmac
import os
from typing import Dict, List, Optional

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import Grant, Publication, User


class InterfolioDebugAPI:
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

        print(f"\nMaking request to: {url}")

        try:
            response = requests.get(url, headers=headers, timeout=30)
            print(f"Response status: {response.status_code}")

            response.raise_for_status()

            if response.text.strip().startswith("<"):
                print(f"HTML response received")
                return None

            data = response.json()
            return data

        except Exception as e:
            print(f"Request error: {e}")
            return None

    def get_user_activities(self, user_id: str) -> List[Dict]:
        """Get activities using the correct nested structure"""

        # Get the response which will be a dict with activity type keys
        response = self._make_request(
            "/activities/-21", f"?data=detailed&userlist={user_id}"
        )

        if not response or not isinstance(response, dict):
            return []

        # The activities are nested under the '-21' key (or similar keys)
        # Extract all activities from all keys in the response
        all_activities = []

        for key, value in response.items():
            # Each key might contain a list of activities or a single activity
            if isinstance(value, list):
                all_activities.extend(value)
            elif isinstance(value, dict) and "activityid" in value:
                all_activities.append(value)

        return all_activities


def test_publication_creation(activity: Dict, user_id: str) -> Optional[Publication]:
    """Test creating a Publication object from activity data"""
    fields = activity.get("fields", {})
    activity_type = fields.get("Type", "")

    if activity_type not in ["Journal Article", "Book"]:
        return None

    print(f"\n  Creating Publication from activity {activity.get('activityid')}")
    print(f"    Type: {activity_type}")
    print(f"    Title: {fields.get('Title', 'N/A')[:80]}")

    # Extract status info
    status_info = {}
    if activity.get("status"):
        if isinstance(activity["status"], list) and len(activity["status"]) > 0:
            status_info = activity["status"][0]

    try:
        pub = Publication(
            user_id=user_id,
            activityid=activity.get("activityid"),
            type=activity_type,
            title=fields.get("Title"),
            journal=fields.get("Journal Title"),
            series_title=fields.get("Series Title"),
            year=str(fields.get("Year")) if fields.get("Year") else None,
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
            status_year=(
                str(status_info.get("year")) if status_info.get("year") else None
            ),
        )
        print(f"    ✓ Publication object created successfully")
        return pub
    except Exception as e:
        print(f"    ✗ Failed to create Publication: {e}")
        return None


def test_grant_creation(activity: Dict, user_id: str) -> Optional[Grant]:
    """Test creating a Grant object from activity data"""
    fields = activity.get("fields", {})

    # Check if this has grant fields
    if not fields.get("Grant ID / Contract ID"):
        return None

    print(f"\n  Creating Grant from activity {activity.get('activityid')}")
    print(f"    Title: {fields.get('Title', 'N/A')[:80]}")
    print(f"    Grant ID: {fields.get('Grant ID / Contract ID')}")

    # Extract status info
    status_info = {}
    if activity.get("status"):
        if isinstance(activity["status"], list) and len(activity["status"]) > 0:
            status_info = activity["status"][0]

    # Extract funding info
    funding_info = {}
    if activity.get("funding"):
        if isinstance(activity["funding"], dict):
            # Get the first funding entry
            for key, value in activity["funding"].items():
                if isinstance(value, dict):
                    funding_info = value
                    break

    try:
        grant = Grant(
            user_id=user_id,
            activityid=activity.get("activityid"),
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
            status_year=(
                str(status_info.get("year")) if status_info.get("year") else None
            ),
        )
        print(f"    ✓ Grant object created successfully")
        return grant
    except Exception as e:
        print(f"    ✗ Failed to create Grant: {e}")
        return None


def debug_user():
    # Load environment
    load_dotenv()
    database_url = os.getenv(
        "DATABASE_URL", "postgresql://admin:secret@localhost/research"
    )

    # Create database connection
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Get Joel Saltz or first non-staff user from database
        user = session.query(User).filter(User.id == "12345").first()
        if not user:
            print("Didn't find user! Grabbing a rando.")
            user = session.query(User).filter(User.employmentstatus != "Staff").first()

        if not user:
            print("No users found in database!")
            return

        print("\n" + "=" * 60)
        print("USER DETAILS FROM DATABASE")
        print("=" * 60)
        print(f"User ID from DB: {user.id}")
        print(f"Name: {user.firstname} {user.lastname}")
        print(f"Email: {user.email}")
        print(f"Position: {user.position}")
        print(f"Status: {user.employmentstatus}")

        # Check existing records
        existing_pubs = (
            session.query(Publication).filter(Publication.user_id == user.id).count()
        )
        existing_grants = session.query(Grant).filter(Grant.user_id == user.id).count()

        print(f"\nExisting records in DB:")
        print(f"  Publications: {existing_pubs}")
        print(f"  Grants: {existing_grants}")

        # Initialize API
        api = InterfolioDebugAPI()

        # First, let's see the raw response structure
        print("\n" + "=" * 60)
        print("RAW API RESPONSE STRUCTURE")
        print("=" * 60)

        raw_response = api._make_request(
            "/activities/-21", f"?data=detailed&userlist={user.id}"
        )

        if raw_response:
            print(f"\nResponse type: {type(raw_response)}")
            print(f"Response keys: {list(raw_response.keys())}")

            # Check what's in the '-21' key
            if "-21" in raw_response:
                data_in_key = raw_response["-21"]
                print(f"\nData in '-21' key:")
                print(f"  Type: {type(data_in_key)}")

                if isinstance(data_in_key, list):
                    print(f"  Number of items: {len(data_in_key)}")
                    if len(data_in_key) > 0:
                        print(f"  First item type: {type(data_in_key[0])}")
                        if isinstance(data_in_key[0], dict):
                            print(
                                f"  First item keys: {list(data_in_key[0].keys())[:15]}"
                            )
                            if "fields" in data_in_key[0]:
                                fields = data_in_key[0]["fields"]
                                print(
                                    f"  First item field keys: {list(fields.keys())[:15]}"
                                )
                                if "Type" in fields:
                                    print(f"  First item Type: {fields['Type']}")
                elif isinstance(data_in_key, dict):
                    print(f"  Dict keys: {list(data_in_key.keys())[:15]}")

        # Now get activities using the correct method
        print("\n" + "=" * 60)
        print("FETCHING ACTIVITIES WITH CORRECT METHOD")
        print("=" * 60)

        activities = api.get_user_activities(user.id)

        print(f"\nTotal activities found: {len(activities)}")

        if not activities:
            print("✗ No activities returned!")

            # Try to manually extract from raw response
            if raw_response and "-21" in raw_response:
                print("\nAttempting manual extraction from '-21' key...")
                if isinstance(raw_response["-21"], list):
                    activities = raw_response["-21"]
                    print(f"✓ Manually extracted {len(activities)} activities")

        if activities:
            # Analyze activity types
            activity_types = {}
            for activity in activities:
                fields = activity.get("fields", {})

                # Check for publication type
                if "Type" in fields:
                    activity_type = fields["Type"]
                    activity_types[activity_type] = (
                        activity_types.get(activity_type, 0) + 1
                    )
                # Check for grant
                elif "Grant ID / Contract ID" in fields:
                    activity_types["Grant"] = activity_types.get("Grant", 0) + 1
                else:
                    activity_types["Unknown"] = activity_types.get("Unknown", 0) + 1

            print(f"\nActivity types found:")
            for act_type, count in sorted(activity_types.items()):
                print(f"  {act_type}: {count}")

            # Test object creation
            print("\n" + "=" * 60)
            print("TESTING OBJECT CREATION")
            print("=" * 60)

            pubs_created = []
            grants_created = []

            for activity in activities[:20]:  # Test first 20
                pub = test_publication_creation(activity, user.id)
                if pub:
                    pubs_created.append(pub)

                grant = test_grant_creation(activity, user.id)
                if grant:
                    grants_created.append(grant)

            print(f"\n" + "=" * 60)
            print("SUMMARY")
            print(f"=" * 60)
            print(f"Successfully created {len(pubs_created)} Publication objects")
            print(f"Successfully created {len(grants_created)} Grant objects")

            # Try to save one of each to database
            if pubs_created and existing_pubs == 0:
                print(f"\nAttempting to save first publication to database...")
                try:
                    session.add(pubs_created[0])
                    session.commit()
                    print("✓ Successfully saved publication!")
                except Exception as e:
                    session.rollback()
                    print(f"✗ Failed to save: {e}")

            if grants_created and existing_grants == 0:
                print(f"\nAttempting to save first grant to database...")
                try:
                    session.add(grants_created[0])
                    session.commit()
                    print("✓ Successfully saved grant!")
                except Exception as e:
                    session.rollback()
                    print(f"✗ Failed to save: {e}")

    finally:
        session.close()


if __name__ == "__main__":
    debug_user()
