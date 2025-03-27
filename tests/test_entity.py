from typing import Any, Dict, List
from .conftest import client


def by_id(dicts: List[Dict[str, Any]], id_: str):
    """Raises if an entity with id is not found"""
    matches = [d for d in dicts if d["id"] == id_]
    assert len(matches) == 1, (id_, dicts)
    return matches[0]


def test_entity_404():
    res = client.get("/entities/banana")
    assert res.status_code == 404, res


def test_entity_fetch():
    res = client.get("/entities/Q7747")
    assert res.status_code == 200, res
    data = res.json()
    assert data["id"] == "Q7747"
    assert data["schema"] == "Person"
    assert "eu_fsf" in data["datasets"]
    assert len(data["referents"]) > 5, data["referents"]
    assert data["last_change"].startswith("20")
    assert data["first_seen"].startswith("20")
    assert data["last_seen"].startswith("20")

    props = data["properties"]
    assert isinstance(props["birthDate"][0], str), props["birthDate"]

    assert "sanctions" in props
    sanc = props["sanctions"][0]
    assert isinstance(sanc, dict), sanc
    assert sanc["schema"] == "Sanction"


def test_entity_not_nested():
    res = client.get("/entities/281d01c426ce39ddf80aa0e46574843c1ba8bfc9?nested=false")
    assert res.status_code == 200
    data = res.json()
    props = data["properties"]
    assert "012a13cddb445def96357093c4ebb30c3c5ab41d" in props["addressEntity"]
    assert len(props["addressEntity"]) == 2
    assert "paymentPayer" not in props


def test_entity_nested():
    res = client.get("/entities/281d01c426ce39ddf80aa0e46574843c1ba8bfc9")
    assert res.status_code == 200
    data = res.json()
    props = data["properties"]
    addr = by_id(props["addressEntity"], "012a13cddb445def96357093c4ebb30c3c5ab41d")
    assert addr["schema"] == "Address"
    assert "Pferdmengesstr" in addr["properties"]["full"][0]
    assert len(props["addressEntity"]) == 2

    assert len(props["paymentPayer"]) == 2
    pmt = by_id(props["paymentPayer"], "98a5b377dffde7307e265a7cc928297e318daaef")
    assert pmt["schema"] == "Payment"
    assert pmt["properties"]["payer"] == ["281d01c426ce39ddf80aa0e46574843c1ba8bfc9"]

    assert len(pmt["properties"]["beneficiary"]) == 2
    beneficiaries = pmt["properties"]["beneficiary"]
    cdu = by_id(beneficiaries, "c326dd8021ee75fe9608f31ecb4e2e7388144102")
    assert cdu["schema"] == "Organization"
    assert cdu["properties"]["name"] == ["CDU"]


def test_adjacent():
    """
    Same data as nested entity endpoint, limited to 10 adjacent per prop
    and structured to include total counts
    """
    res = client.get("/entities/281d01c426ce39ddf80aa0e46574843c1ba8bfc9/adjacent")
    assert res.status_code == 200
    data = res.json()

    entity = data["entity"]
    assert entity["caption"] == "Herr Christoph Alexander Kahl"

    adjacent = data["adjacent"]
    addr = by_id(
        adjacent["addressEntity"]["results"], "012a13cddb445def96357093c4ebb30c3c5ab41d"
    )
    assert addr["schema"] == "Address"
    assert "Pferdmengesstr" in addr["properties"]["full"][0]
    assert len(adjacent["addressEntity"]["results"]) == 2
    assert adjacent["addressEntity"]["total"]["value"] == 2
    assert adjacent["addressEntity"]["limit"] == 10
    assert adjacent["addressEntity"]["offset"] == 0

    payments = adjacent["paymentPayer"]["results"]
    assert len(payments) == 2
    assert adjacent["paymentPayer"]["total"]["value"] == 2
    pmt = by_id(payments, "98a5b377dffde7307e265a7cc928297e318daaef")
    assert pmt["schema"] == "Payment"
    # The interstitial still refers to the root, but doesn't nest it.
    assert pmt["properties"]["payer"] == ["281d01c426ce39ddf80aa0e46574843c1ba8bfc9"]

    assert len(pmt["properties"]["beneficiary"]) == 2
    beneficiaries = pmt["properties"]["beneficiary"]
    cdu = by_id(beneficiaries, "c326dd8021ee75fe9608f31ecb4e2e7388144102")
    assert cdu["schema"] == "Organization"
    assert cdu["properties"]["name"] == ["CDU"]


def test_adjacent_limit():
    """Get the first of two adjacent entities for outgoing and incoming edges"""
    res = client.get(
        "/entities/281d01c426ce39ddf80aa0e46574843c1ba8bfc9/adjacent?limit=1"
    )
    assert res.status_code == 200
    data = res.json()

    adjacent = data["adjacent"]
    addresses = adjacent["addressEntity"]["results"]
    assert len(addresses) == 1
    by_id(addresses, "582e64e961eaf537d8a80856520d601519a5fb98")
    assert adjacent["addressEntity"]["total"]["value"] == 2
    assert adjacent["addressEntity"]["limit"] == 1

    payments = adjacent["paymentPayer"]["results"]
    assert len(payments) == 1
    assert adjacent["paymentPayer"]["total"]["value"] == 2
    pmt = by_id(payments, "98a5b377dffde7307e265a7cc928297e318daaef")

    beneficiaries = pmt["properties"]["beneficiary"]
    # other side of interstitial entities isn't paginated
    assert len(beneficiaries) == 2
    # they're all nested
    len([b for b in beneficiaries if b["schema"] == "Organization"]) == 2
    fake_org = by_id(beneficiaries, "fake-org")
    assert fake_org["properties"]["name"] == ["Fake"]


def test_adjacent_offset():
    """Get the second of two adjacent entities for outgoing and incoming edges"""
    res = client.get(
        "/entities/281d01c426ce39ddf80aa0e46574843c1ba8bfc9/adjacent?limit=1&offset=1"
    )
    assert res.status_code == 200
    data = res.json()

    adjacent = data["adjacent"]
    addresses = adjacent["addressEntity"]["results"]
    by_id(addresses, "012a13cddb445def96357093c4ebb30c3c5ab41d")
    assert len(addresses) == 1
    assert adjacent["addressEntity"]["total"]["value"] == 2
    assert adjacent["addressEntity"]["limit"] == 1
    assert adjacent["addressEntity"]["offset"] == 1

    payments = adjacent["paymentPayer"]["results"]
    assert len(payments) == 1
    assert adjacent["paymentPayer"]["total"]["value"] == 2
    pmt = by_id(payments, "8928105b8d3748805295a0fb01ae57cc37be30a5")

    beneficiaries = pmt["properties"]["beneficiary"]
    assert len(beneficiaries) == 1
    cdu = by_id(beneficiaries, "c326dd8021ee75fe9608f31ecb4e2e7388144102")
    assert cdu["properties"]["name"] == ["CDU"]


def test_adjacent_prop():
    res = client.get(
        "/entities/281d01c426ce39ddf80aa0e46574843c1ba8bfc9/adjacent/address"
    )
    assert res.status_code == 404

    res = client.get(
        "/entities/281d01c426ce39ddf80aa0e46574843c1ba8bfc9/adjacent/addressEntity"
    )
    assert res.status_code == 200
    data = res.json()
    assert data["limit"] == 10
    assert data["offset"] == 0
    addr = by_id(data["results"], "012a13cddb445def96357093c4ebb30c3c5ab41d")
    assert addr["schema"] == "Address"
    assert "Pferdmengesstr" in addr["properties"]["full"][0]
    assert len(data["results"]) == 2
    assert data["total"]["value"] == 2

    res = client.get(
        "/entities/281d01c426ce39ddf80aa0e46574843c1ba8bfc9/adjacent/paymentPayer"
    )
    assert res.status_code == 200
    data = res.json()
    assert len(data["results"]) == 2
    assert data["total"]["value"] == 2
    pmt = by_id(data["results"], "98a5b377dffde7307e265a7cc928297e318daaef")
    assert pmt["schema"] == "Payment"
    # The interstitial still refers to the root, but doesn't nest it.
    assert pmt["properties"]["payer"] == ["281d01c426ce39ddf80aa0e46574843c1ba8bfc9"]

    assert len(pmt["properties"]["beneficiary"]) == 2
    beneficiaries = pmt["properties"]["beneficiary"]
    cdu = by_id(beneficiaries, "c326dd8021ee75fe9608f31ecb4e2e7388144102")
    assert cdu["schema"] == "Organization"
    assert cdu["properties"]["name"] == ["CDU"]


def test_adjacent_prop_limit():
    """Get the first of two adjacent entities"""
    res = client.get(
        "/entities/281d01c426ce39ddf80aa0e46574843c1ba8bfc9/adjacent/addressEntity?limit=1"
    )
    assert res.status_code == 200
    data = res.json()
    assert data["limit"] == 1
    by_id(data["results"], "582e64e961eaf537d8a80856520d601519a5fb98")
    assert len(data["results"]) == 1
    assert data["total"]["value"] == 2

    res = client.get(
        "/entities/281d01c426ce39ddf80aa0e46574843c1ba8bfc9/adjacent/paymentPayer?limit=1"
    )
    assert res.status_code == 200
    data = res.json()
    assert len(data["results"]) == 1
    assert data["total"]["value"] == 2
    pmt = by_id(data["results"], "98a5b377dffde7307e265a7cc928297e318daaef")

    beneficiaries = pmt["properties"]["beneficiary"]
    # other side of interstitial entities isn't paginated
    assert len(beneficiaries) == 2
    # they're all nested
    len([b for b in beneficiaries if b["schema"] == "Organization"]) == 2
    fake_org = by_id(beneficiaries, "fake-org")
    assert fake_org["properties"]["name"] == ["Fake"]


def test_adjacent_prop_offset():
    """Get the second of two adjacent entities"""
    res = client.get(
        "/entities/281d01c426ce39ddf80aa0e46574843c1ba8bfc9/adjacent/addressEntity?limit=1&offset=1"
    )
    assert res.status_code == 200
    data = res.json()
    assert data["limit"] == 1
    assert data["offset"] == 1
    by_id(data["results"], "012a13cddb445def96357093c4ebb30c3c5ab41d")
    assert len(data["results"]) == 1
    assert data["total"]["value"] == 2
    res = client.get(
        "/entities/281d01c426ce39ddf80aa0e46574843c1ba8bfc9/adjacent/paymentPayer?limit=1&offset=1"
    )
    assert res.status_code == 200
    data = res.json()

    assert len(data["results"]) == 1
    assert data["total"]["value"] == 2
    pmt = by_id(data["results"], "8928105b8d3748805295a0fb01ae57cc37be30a5")

    beneficiaries = pmt["properties"]["beneficiary"]
    assert len(beneficiaries) == 1
    cdu = by_id(beneficiaries, "c326dd8021ee75fe9608f31ecb4e2e7388144102")
    assert cdu["properties"]["name"] == ["CDU"]
