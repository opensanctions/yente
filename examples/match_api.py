import requests

# The OpenSanctions service API. This endpoint will only do sanctions checks.
# In order to check both sanctions and PEPs, use `/match/default`.
URL = "https://api.opensanctions.org/match/sanctions"

EXAMPLE_1 = {
    "schema": "Person",
    "properties": {
        "name": ["Arkadiii Romanovich Rotenberg", "Ротенберг Аркадий"],
        "birthDate": ["1951"],
    },
}

EXAMPLE_2 = {
    "schema": "Company",
    "properties": {
        "name": ["Stroygazmontazh"],
        "jurisdiction": ["Russia"],
    },
}

BATCH = {"queries": {"q1": EXAMPLE_1, "q2": EXAMPLE_2}}


response = requests.post(URL, json=BATCH)
response.raise_for_status()
print(response.json())
