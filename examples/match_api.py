import os
import requests
from pprint import pprint

# The OpenSanctions service API. This endpoint will only do sanctions checks.
URL = "https://api.opensanctions.org/match/sanctions"
API_KEY = os.environ.get("OPENSANCTIONS_API_KEY")

# A query for a person with a specific name and birth date. Note multiple names given
# in different alphabets:
EXAMPLE_1 = {
    "schema": "Person",
    "properties": {
        "name": ["Arkadiii Romanovich Rotenberg", "Ротенберг Аркадий"],
        "birthDate": ["1951"],
    },
}

# Similarly, a company search using just a name and jurisdiction.
EXAMPLE_2 = {
    "schema": "Company",
    "properties": {
        "name": ["Stroygazmontazh"],
        "jurisdiction": ["Russia"],
    },
}

# We put both of these queries into a matching batch, giving each of them an
# ID that we can recognize it by later:
BATCH = {"queries": {"q1": EXAMPLE_1, "q2": EXAMPLE_2}}

# Send the batch off to the API and raise an exception for a non-OK response code.
headers = {"Authorization": f"Apikey {API_KEY}"}
response = requests.post(URL, json=BATCH, headers=headers)
response.raise_for_status()

responses = response.json().get("responses")

# The responses will include a set of results for each entity, and a parsed version of
# the original query:
example_1_response = responses.get("q1")
example_2_response = responses.get("q2")

# You can use the returned query to debug if the API correctly parsed and interpreted
# the queries you provided. If any of the fields or values are missing, it's an
# indication their format wasn't accepted by the system.
pprint(example_2_response["query"])

# The results are a list of entities, formatted using the same structure as your
# query examples. By default, the API will at most return five potential matches.
for result in example_2_response["results"]:
    pprint(result)
