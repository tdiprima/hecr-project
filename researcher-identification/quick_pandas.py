import os
from contextlib import contextmanager

import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()


@contextmanager
def open_db(name):
    conn = psycopg2.connect(
        host="localhost",
        database=name,
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        port=5432,
    )
    try:
        yield conn
    finally:
        conn.close()


with open_db("research") as conn:
    query = """
    SELECT h.firstname, h.lastname, p.title as publication_title, g.title as grant_title
    FROM hecr h
    LEFT JOIN publications p ON h.id = p.user_id
    LEFT JOIN grants g ON h.id = g.user_id
    WHERE p.id IS NOT NULL OR g.id IS NOT NULL
    """
    df = pd.read_sql(query, conn)
    df.to_csv("hecr_export.csv", index=False)
