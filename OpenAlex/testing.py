import requests

headers = {"User-Agent": "BearBot/0.1 (bear@example.com)"}  # put your email here

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
