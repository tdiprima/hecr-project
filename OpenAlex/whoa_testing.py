# https://docs.openalex.org/
import requests

url = "https://api.openalex.org/authors"
response = requests.get(url)
response.raise_for_status()
print(response.json())
