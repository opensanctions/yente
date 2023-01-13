from requests import Session

from .util import find_result_id, first_result_id


def test_search_putin(http: Session, search_url: str):
    resp = http.get(search_url, params={"q": "vladimir putin"})
    assert find_result_id(resp.json(), "Q7747"), resp.json()


def test_search_falah_taha(http: Session, search_url: str):
    resp = http.get(search_url, params={"q": "Falah Jaber Taha"})
    assert find_result_id(resp.json(), "Q17544625"), resp.json()


def test_search_fuzzy_barrack(http: Session, search_url: str):
    resp = http.get(search_url, params={"q": "Barrack~ Obama", "fuzzy": "true"})
    assert first_result_id(resp.json(), "Q76"), resp.json()


def test_search_fuzzy_barock(http: Session, search_url: str):
    resp = http.get(search_url, params={"q": "Barock~ Obama", "fuzzy": "true"})
    assert first_result_id(resp.json(), "Q76"), resp.json()
