#!/usr/bin/env python3
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine

from models import Base


def create_database_tables():
    load_dotenv()

    database_url = os.getenv("DATABASE_URL")

    print(f"Connecting to database: {database_url.split('@')[1]}")

    engine = create_engine(database_url)

    print("Creating tables...")
    Base.metadata.create_all(engine)
    print("Tables created successfully!")

    print("Available tables:")
    for table_name in Base.metadata.tables.keys():
        print(f"  - {table_name}")


if __name__ == "__main__":
    create_database_tables()
