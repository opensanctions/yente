from requests import Session
from urllib.parse import urljoin


def test_fetch_putin(http: Session, service_url: str):
    resp = http.get(urljoin(service_url, f"/entities/Q7747"))
    assert resp.ok, resp.text
    data = resp.json()
    assert data["id"] == "Q7747", data["id"]
