import sys
import requests
from pprint import pprint

# The OpenSanctions service API. This endpoint will only do sanctions checks.
URL = "https://api-test.opensanctions.org/match/sanctions"
URL = "http://localhost:9000/match/sanctions"

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

EXAMPLE_3 = {
    "properties": {
        "name": [
            "ERMAKOV Valery Nikolaevich",
            "Ermacov Valeryi Nycolaevych",
            "Ermakov Valerij Nikolaevich",
            "Ermakov Valerij Nikolaevič",
            "Ermakov Valerijj Nikolaevich",
            "Ermakov Valeriy Nikolaevich",
            "Ermakov Valery Nykolaevych",
            "Ermakov Valeryi Nykolaevych",
            "Ermakov Valeryy Nikolaevich",
            "Ermakov Valeryy Nykolaevych",
            "Ermakov Valerȳĭ Nȳkolaevȳch",
            "Iermakov Valerii Mykolaiovych",
            "Jermakov Valerij Mikolajovich",
            "Jermakov Valerij Mikolajovič",
            "Jermakov Valerij Mykolajovyč",
            "Yermakov Valerii Mykolaiovych",
            "Yermakov Valerij Mykolajovych",
            "Yermakov Valeriy Mykolayovych",
            "Êrmakov Valerìj Mikolajovič",
            "ЕРМАКОВ Валерий Николаевич",
        ]
    },
    "schema": "Person",
}

# We put both of these queries into a matching batch, giving each of them an
# ID that we can recognize it by later:
BATCH = {"queries": {"q1": EXAMPLE_1, "q2": EXAMPLE_2, "q3": EXAMPLE_3}}

# Send the batch off to the API and raise an exception for a non-OK response code.
response = requests.post(URL, json=BATCH)
if not response.ok:
    pprint(response.json())
    sys.exit(1)

responses = response.json().get("responses")
for key, results in responses.items():
    print(results["query"])
    for res in results["results"]:
        print(" ->", res["schema"], res["caption"], res["score"], res["match"])
