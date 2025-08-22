#!/usr/bin/env python3
"""
Query users who have records in publications or grants tables.
"""

import signal
import sys
import psycopg2
from psycopg2 import sql


def signal_handler(sig, frame):
    """Handle Ctrl-C gracefully."""
    print("\nOperation cancelled by user.")
    sys.exit(0)


def get_active_users():
    """Get users who have publications or grants."""
    try:
        # Register signal handler for Ctrl-C
        signal.signal(signal.SIGINT, signal_handler)
        
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host="localhost",
            database="research", 
            user="admin",
            password="secret"
        )
        
        cursor = conn.cursor()
        
        # Query users who have publications or grants
        query = """
        SELECT DISTINCT u.firstname, u.lastname
        FROM users u
        WHERE (EXISTS (
            SELECT 1 FROM publications p WHERE p.user_id = u.id
        ) OR EXISTS (
            SELECT 1 FROM grants g WHERE g.user_id = u.id
        ))
        AND u.firstname != 'Demo'
        AND u.lastname != 'zzFaculty'
        ORDER BY u.lastname, u.firstname;
        """
        
        cursor.execute(query)
        users = cursor.fetchall()
        
        print(f"Found {len(users)} users with publications or grants:\n")
        
        for firstname, lastname in users:
            print(f"{firstname} {lastname}")
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    get_active_users()