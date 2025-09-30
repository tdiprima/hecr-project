"""
Modular utility functions for syncing activities (publications and grants).
"""
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Set, Type

from sqlalchemy.orm import Session

from models import Grant, Publication


@dataclass
class ActivityTracker:
    """Tracks IDs seen during synchronization to identify stale records for deletion."""
    seen_users: Set[str] = field(default_factory=set)
    seen_publications: Set[int] = field(default_factory=set)
    seen_grants: Set[int] = field(default_factory=set)
    
    def track_user(self, user_id: str):
        """Track a user ID as seen during sync."""
        if user_id:
            self.seen_users.add(user_id)
    
    def track_publication(self, activity_id: int):
        """Track a publication activity ID as seen during sync."""
        if activity_id:
            self.seen_publications.add(activity_id)
    
    def track_grant(self, activity_id: int):
        """Track a grant activity ID as seen during sync."""
        if activity_id:
            self.seen_grants.add(activity_id)
    
    def clear(self):
        """Clear all tracked IDs."""
        self.seen_users.clear()
        self.seen_publications.clear()
        self.seen_grants.clear()
    
    @property
    def summary(self) -> Dict[str, int]:
        """Get a summary of tracked items."""
        return {
            "users": len(self.seen_users),
            "publications": len(self.seen_publications),
            "grants": len(self.seen_grants),
        }


def truncate_field(value: Optional[Any], max_length: int) -> Optional[str]:
    """
    Truncate a field to max_length if it's too long.
    
    Args:
        value: The value to truncate (can be any type)
        max_length: Maximum allowed length
        
    Returns:
        Truncated string or original value if not a string or within limits
    """
    if value is None:
        return None
    
    str_value = str(value) if not isinstance(value, str) else value
    
    if len(str_value) > max_length:
        return str_value[:max_length]
    
    return str_value


def delete_stale_activities(
    session: Session,
    model_class: Type[Any],
    id_field: str,
    seen_ids: Set[Any]
) -> int:
    """
    Delete records from database that weren't seen in the API response.
    
    Args:
        session: SQLAlchemy database session
        model_class: The model class to delete from (Publication or Grant)
        id_field: The field name to use for ID comparison (usually 'activityid')
        seen_ids: Set of IDs that were seen in the API
        
    Returns:
        Number of records deleted
    """
    # Get all existing IDs from database
    existing_records = session.query(model_class).all()
    
    deleted_count = 0
    for record in existing_records:
        record_id = getattr(record, id_field)
        # If this ID wasn't seen in the API, delete it
        if record_id and record_id not in seen_ids:
            session.delete(record)
            deleted_count += 1
            
            # Log what we're deleting for audit trail
            if hasattr(record, 'title'):
                title = getattr(record, 'title', 'Untitled')
                logging.debug(f"Deleting {model_class.__name__} {record_id}: {title[:50] if title else 'Untitled'}")
    
    return deleted_count


def create_publication_from_api(activity_data: Dict, user_id: str) -> Optional[Publication]:
    """
    Create a Publication object from Interfolio API activity data.
    
    Args:
        activity_data: Raw activity data from the API
        user_id: The user ID this publication belongs to
        
    Returns:
        Publication object or None if not a valid publication type
    """
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

        # Create publication with truncated fields to match database constraints
        return Publication(
            user_id=user_id,
            activityid=activity_data.get("activityid"),
            type=truncate_field(activity_type, 50),
            title=truncate_field(fields.get("Title"), 255),
            journal=truncate_field(fields.get("Journal Title"), 255),
            series_title=truncate_field(fields.get("Series Title"), 255),
            year=truncate_field(year, 4),
            month_season=truncate_field(fields.get("Month / Season"), 50),
            publisher=truncate_field(fields.get("Publisher"), 255),
            publisher_city_state=truncate_field(
                fields.get("Publisher City and State"), 255
            ),
            publisher_country=truncate_field(fields.get("Publisher Country"), 100),
            volume=truncate_field(fields.get("Volume"), 50),
            issue_number=truncate_field(fields.get("Issue Number / Edition"), 50),
            page_numbers=truncate_field(
                fields.get("Page Number(s) or Number of Pages"), 50
            ),
            isbn=truncate_field(fields.get("ISBN"), 20),
            issn=truncate_field(fields.get("ISSN"), 20),
            doi=truncate_field(fields.get("DOI"), 255),
            url=truncate_field(fields.get("URL"), 500),
            description=fields.get("Description"),  # Text field, no truncation
            origin=truncate_field(fields.get("Origin"), 50),
            status=truncate_field(
                status_info.get("status") if status_info else None, 50
            ),
            term=truncate_field(
                status_info.get("term") if status_info else None, 50
            ),
            status_year=truncate_field(
                (
                    str(status_info.get("year"))
                    if status_info and status_info.get("year")
                    else None
                ),
                4,
            ),
        )
    except Exception as e:
        logging.error(f"Error creating publication: {e}")
        return None


def create_grant_from_api(activity_data: Dict, user_id: str) -> Optional[Grant]:
    """
    Create a Grant object from Interfolio API activity data.
    
    Args:
        activity_data: Raw activity data from the API
        user_id: The user ID this grant belongs to
        
    Returns:
        Grant object or None if not a valid grant
    """
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

        # Create grant with truncated fields to match database constraints
        return Grant(
            user_id=user_id,
            activityid=activity_data.get("activityid"),
            title=truncate_field(fields.get("Title"), 255),
            sponsor=truncate_field(fields.get("Sponsor"), 255),
            grant_id=truncate_field(fields.get("Grant ID / Contract ID"), 100),
            award_date=fields.get("Award Date"),
            start_date=fields.get("Start Date"),
            end_date=fields.get("End Date"),
            period_length=fields.get("Period Length"),
            period_unit=truncate_field(fields.get("Period Unit"), 50),
            indirect_funding=fields.get("Indirect Funding"),
            indirect_cost_rate=truncate_field(fields.get("Indirect Cost Rate"), 50),
            total_funding=truncate_field(
                str(total_funding) if total_funding else None, 50
            ),
            total_direct_funding=truncate_field(
                (
                    str(fields.get("Total Direct Funding"))
                    if fields.get("Total Direct Funding")
                    else None
                ),
                50,
            ),
            currency_type=truncate_field(fields.get("Currency Type"), 10),
            description=fields.get("Description"),  # Text field, no truncation
            abstract=fields.get("Abstract"),  # Text field, no truncation
            number_of_periods=fields.get("Number of Periods"),
            url=truncate_field(fields.get("URL"), 500),
            status=truncate_field(
                status_info.get("status") if status_info else None, 50
            ),
            term=truncate_field(status_info.get("term") if status_info else None, 50),
            status_year=truncate_field(
                (
                    str(status_info.get("year"))
                    if status_info and status_info.get("year")
                    else None
                ),
                4,
            ),
        )
    except Exception as e:
        logging.error(f"Error creating grant: {e}")
        return None


@dataclass
class SyncStats:
    """Statistics tracker for sync operations."""
    users_processed: int = 0
    publications_added: int = 0
    publications_updated: int = 0
    publications_deleted: int = 0
    grants_added: int = 0
    grants_updated: int = 0
    grants_deleted: int = 0
    errors: int = 0
    
    def log_summary(self):
        """Log a summary of the sync stats."""
        logging.info("Sync Summary:")
        logging.info(f"  Users processed: {self.users_processed}")
        logging.info(f"  Publications: +{self.publications_added}, ~{self.publications_updated}, -{self.publications_deleted}")
        logging.info(f"  Grants: +{self.grants_added}, ~{self.grants_updated}, -{self.grants_deleted}")
        if self.errors > 0:
            logging.warning(f"  Errors encountered: {self.errors}")
