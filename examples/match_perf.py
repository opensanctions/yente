import time
import requests

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

BATCH = {"queries": {"q1": EXAMPLE_1, "q2": EXAMPLE_2, "q3": EXAMPLE_3}}

for fuzzy in (True, False):
    start = time.time()
    for i in range(100):
        params = {"fuzzy": fuzzy}
        response = requests.post(URL, json=BATCH, params=params)
        if not response.ok:
            print("FAIL", response.status_code)
            continue
    end = time.time()
    print("TOOK", end - start, "FUZZY", fuzzy)
