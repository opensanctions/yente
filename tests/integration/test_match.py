from requests import Session

from .util import find_result_id


def test_match_falah_taha(http: Session, match_url: str):
    query = {"schema": "Person", "properties": {"name": "Falah Jaber Taha"}}
    resp = http.post(match_url, json={'queries': {'q1': query}})
    assert resp.status_code == 200, resp
    qres = resp.json()['responses']['q1']
    assert find_result_id(qres, 'Q17544625'), qres
