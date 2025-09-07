import pandas as pd
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="research",
    user="your_username",
    password="your_password",
    port=5432,
)
query = "SELECT h.firstname, h.lastname, p.title as publication_title, g.title as grant_title FROM hecr h LEFT JOIN publications p ON h.id = p.user_id LEFT JOIN grants g ON h.id = g.user_id WHERE p.id IS NOT NULL OR g.id IS NOT NULL"
df = pd.read_sql(query, conn)
df.to_csv("hecr_export.csv", index=False)

conn.close()
