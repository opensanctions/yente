import json
from requests import Session

from .util import first_result_id

ROTENBERG = {
    "schema": "Person",
    "properties": {
        "name": ["Arkadiii Romanovich Rotenberg", "Ротенберг Аркадий"],
        "birthDate": ["1951"],
    },
}

SGM = {
    "schema": "Company",
    "properties": {
        "name": ["Stroygazmontazh"],
        "jurisdiction": ["Russia"],
    },
}


def test_match_falah_taha(http: Session, match_url: str):
    query = {"schema": "Person", "properties": {"name": "Falah Jaber Taha"}}
    resp = http.post(match_url, json={"queries": {"q1": query}})
    assert resp.status_code == 200, resp.text
    qres = resp.json()["responses"]["q1"]
    assert first_result_id(qres, "Q17544625"), qres


def test_match_rotenberg(http: Session, match_url: str):
    resp = http.post(match_url, json={"queries": {"q1": ROTENBERG, "q2": SGM}})
    assert resp.status_code == 200, resp.text
    qres = resp.json()["responses"]["q1"]
    assert first_result_id(qres, "Q4398633"), qres

    # SGM company ID:
    qres = resp.json()["responses"]["q2"]["results"][0]
    assert "1077762942212" in json.dumps(qres)
