import os
from pprint import pprint

import requests

# The OpenSanctions service API. This endpoint will only do sanctions checks.
URL = "https://api.opensanctions.org/match/sanctions"
API_KEY = os.environ.get("OPENSANCTIONS_API_KEY")

# A query for a person with a specific name and birth date. Note multiple names given
# in different alphabets:
EXAMPLE = {
    "schema": "Person",
    "properties": {
        "name": ["Arkadiii Romanovich Rotenberg", "Ротенберг Аркадий"],
        "birthDate": ["1951"],
    },
}

# We give the query an ID that we can recognize in the response:
QUERY = {"queries": {"q": EXAMPLE}}

# Configure an API key for the service. This is required for the hosted API.
headers = {"Authorization": f"Apikey {API_KEY}"}

# This configures the scoring system. "fuzzy" is related only to the pre-retrieval
# of entities and can be turned off for a performance boost.
params = {"algorithm": "best", "fuzzy": "false"}

# Send the query and raise an exception for a non-OK response code.
response = requests.post(URL, json=QUERY, headers=headers, params=params)
response.raise_for_status()

# The responses will include a set of results for each entity, and a parsed version of
# the original query:
example_response = response.json().get("responses").get("q")

# You can use the returned query to debug if the API correctly parsed and interpreted
# the queries you provided. If any of the fields or values are missing, it's an
# indication their format wasn't accepted by the system.
pprint(example_response["query"])

# The results are a list of entities, formatted using the same structure as your
# query examples. By default, the API will at most return five potential matches.
for result in example_response["results"]:
    pprint(result)
