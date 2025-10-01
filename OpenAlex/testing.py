import os

import requests

email = os.getenv("EMAIL", "bear@example.com")
headers = {"User-Agent": f"BearBot/0.1 ({email})"}

response = requests.get(
    "https://api.openalex.org/authors",
    params={
        "filter": "last_known_institution.display_name.search:stonybrook",
        "per_page": 5,
    },
    headers=headers,
)

response.raise_for_status()
print(response.json())
