import os
import time
import random
import logging
import requests
from urllib.parse import urljoin
from requests import JSONDecodeError
from concurrent.futures import ThreadPoolExecutor

log = logging.getLogger("load_test")
session = requests.Session()

HOST = "https://api.test.opensanctions.org/"
# HOST = "http://localhost:9000/"
API_KEY = os.environ.get("OPENSANCTIONS_API_KEY")
if API_KEY:
    session.headers["Authorization"] = f"Apikey {API_KEY}"

QUERIES = ["vladimir putin", "hamas", "ukraine", "petr~2 aven", "bla/blubb"]

ENTITY_IDS = set(["Q7747", "Q19077", "Q154797"])

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


def match_api():
    BATCH = {"queries": {"q1": EXAMPLE_1, "q2": EXAMPLE_2, "q3": EXAMPLE_3}}
    url = urljoin(HOST, "/match/sanctions")
    response = session.post(url, json=BATCH)
    if not response.ok:
        log.error("Failed: %s (%s)", url, response.status_code)
        return
    try:
        responses = response.json().get("responses")
    except JSONDecodeError:
        log.error("Invalid server response")
        return
    for resp in responses.values():
        # print(list(resp.keys()))
        for result in resp.get("results", []):
            ENTITY_IDS.add(result["id"])
            log.info("Match: %s", result["id"])


def search_api():
    url = urljoin(HOST, "/search/default")
    q = random.choice(QUERIES)
    params = {"q": q, "limit": random.randint(0, 500)}
    log.info("Query: %s (limit %d)", q, params["limit"])
    response = session.get(url, params=params)
    if not response.ok:
        log.error("Failed: %s (%s)", url, response.status_code)
        return
    try:
        data = response.json()
    except JSONDecodeError:
        log.error("Invalid server response")
        return
    if "results" not in data:
        log.error("Invalid response: %r", data)
        return
    for result in data["results"]:
        ENTITY_IDS.add(result["id"])


def entity_api():
    entity_id = random.choice(list(ENTITY_IDS))
    url = urljoin(HOST, f"/entities/{entity_id}")
    log.info("Entity: %s", entity_id)
    response = session.get(url)
    if not response.ok:
        log.error("Failed: %s (%s)", url, response.status_code)


APIS = [match_api, search_api, entity_api, entity_api]
# APIS = [search_api, entity_api, entity_api]
# APIS = [match_api]


def pool_target(num):
    api = random.choice(APIS)
    try:
        api()
    except requests.exceptions.ConnectionError as err:
        log.warning("HTTP error: %s", err)
        time.sleep(0.1)


def load_test():
    queue = []
    for i in range(0, 100000):
        queue.append(i)

    with ThreadPoolExecutor(max_workers=10) as pool:
        for res in pool.map(pool_target, queue):
            pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load_test()
