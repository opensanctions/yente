from .conftest import client


def filter_id(dicts, id_):
    return [d for d in dicts if d["id"] == id_]


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
    assert sanc["schema"] == "Sanction", sanc


def test_entity_not_nested():
    res = client.get("/entities/281d01c426ce39ddf80aa0e46574843c1ba8bfc9?nested=false")
    assert res.status_code == 200, res
    data = res.json()
    props = data["properties"]
    assert "012a13cddb445def96357093c4ebb30c3c5ab41d" in props["addressEntity"], props
    assert len(props["addressEntity"]) == 2, props
    assert "ownershipOwner" not in props


def test_entity_nested():
    res = client.get("/entities/281d01c426ce39ddf80aa0e46574843c1ba8bfc9")
    assert res.status_code == 200, res
    data = res.json()
    props = data["properties"]
    addr = filter_id(
        props["addressEntity"], "012a13cddb445def96357093c4ebb30c3c5ab41d"
    )[0]
    assert addr["schema"] == "Address", addr
    assert "Pferdmengesstr" in addr["properties"]["full"][0], addr
    assert len(props["addressEntity"]) == 2, props

    assert len(props["ownershipOwner"]) == 2, props
    own = filter_id(
        props["ownershipOwner"], "b89c677ed30888623500bfbfdb384d3eec259070"
    )[0]
    assert own["schema"] == "Ownership", own
    assert own["properties"]["owner"] == [
        "281d01c426ce39ddf80aa0e46574843c1ba8bfc9"
    ], own

    assert len(own["properties"]["asset"]) == 1, own
    asset = own["properties"]["asset"][0]
    assert asset["schema"] == "Company", asset
    assert asset["properties"]["name"][0] == "Fake Invest GmbH", asset


def test_adjacent():
    res = client.get("/entities/281d01c426ce39ddf80aa0e46574843c1ba8bfc9/adjacent")
    assert res.status_code == 200, res
    data = res.json()
    assert data["limit"] == 10, data
    assert data["offset"] == 0, data

    entity = data["entity"]
    assert entity["caption"] == "Herr Christoph Alexander Kahl", entity

    props = data["adjacent"]
    addr = filter_id(
        props["addressEntity"]["results"], "012a13cddb445def96357093c4ebb30c3c5ab41d"
    )[0]
    assert addr["schema"] == "Address", addr
    assert "Pferdmengesstr" in addr["properties"]["full"][0], addr
    assert len(props["addressEntity"]["results"]) == 2, props
    assert props["addressEntity"]["total"]["value"] == 2, props

    assert len(props["ownershipOwner"]["results"]) == 2, props
    assert props["ownershipOwner"]["total"]["value"] == 2, props
    own = filter_id(
        props["ownershipOwner"]["results"], "b89c677ed30888623500bfbfdb384d3eec259070"
    )[0]
    assert own["schema"] == "Ownership", own
    assert own["properties"]["owner"] == [
        "281d01c426ce39ddf80aa0e46574843c1ba8bfc9"
    ], own

    assert len(own["properties"]["asset"]) == 1, own
    asset = own["properties"]["asset"][0]
    assert asset["schema"] == "Company", asset
    assert asset["properties"]["name"][0] == "Fake Invest GmbH", asset


def test_adjacent_limit():
    """Get the first of two adjacent entities"""
    res = client.get(
        "/entities/281d01c426ce39ddf80aa0e46574843c1ba8bfc9/adjacent?limit=1"
    )
    assert res.status_code == 200, res
    data = res.json()
    assert data["limit"] == 1, data

    props = data["adjacent"]
    addresses = props["addressEntity"]["results"]
    assert (
        len(filter_id(addresses, "582e64e961eaf537d8a80856520d601519a5fb98")) == 1
    ), props
    assert len(addresses) == 1, props
    assert props["addressEntity"]["total"]["value"] == 2, props

    ownerships = props["ownershipOwner"]["results"]
    assert len(ownerships) == 1, props
    assert props["ownershipOwner"]["total"]["value"] == 2, props
    assert (
        len(filter_id(ownerships, "b89c677ed30888623500bfbfdb384d3eec259070")) == 1
    ), props

    asset = ownerships[0]["properties"]["asset"][0]
    assert asset["properties"]["name"][0] == "Fake Invest GmbH", asset


def test_adjacent_offset():
    """Get the second of two adjacent entities"""
    res = client.get(
        "/entities/281d01c426ce39ddf80aa0e46574843c1ba8bfc9/adjacent?limit=1&offset=1"
    )
    assert res.status_code == 200, res
    data = res.json()
    assert data["limit"] == 1, data
    assert data["offset"] == 1, data

    props = data["adjacent"]
    addresses = props["addressEntity"]["results"]
    assert (
        len(filter_id(addresses, "012a13cddb445def96357093c4ebb30c3c5ab41d")) == 1
    ), props
    assert len(addresses) == 1, props
    assert props["addressEntity"]["total"]["value"] == 2, props

    ownerships = props["ownershipOwner"]["results"]
    assert len(ownerships) == 1, props
    assert props["ownershipOwner"]["total"]["value"] == 2, props
    assert (
        len(filter_id(ownerships, "b89c677ed30888623500bbbbbbbbbbbbbbbbbbbb")) == 1
    ), props
    asset = ownerships[0]["properties"]["asset"][0]
    print("asset", asset)
    assert asset["properties"]["name"][0] == "Fake Holding GmbH", asset


def test_adjacent_prop():
    res = client.get(
        "/entities/281d01c426ce39ddf80aa0e46574843c1ba8bfc9/adjacent/address"
    )
    assert res.status_code == 404, res

    res = client.get(
        "/entities/281d01c426ce39ddf80aa0e46574843c1ba8bfc9/adjacent/addressEntity"
    )
    assert res.status_code == 200, res
    data = res.json()
    assert data["limit"] == 10, data
    assert data["offset"] == 0, data
    addr = filter_id(data["results"], "012a13cddb445def96357093c4ebb30c3c5ab41d")[0]
    assert addr["schema"] == "Address", addr
    assert "Pferdmengesstr" in addr["properties"]["full"][0], addr
    assert len(data["results"]) == 2, data
    assert data["total"]["value"] == 2, data

    res = client.get(
        "/entities/281d01c426ce39ddf80aa0e46574843c1ba8bfc9/adjacent/ownershipOwner"
    )
    assert res.status_code == 200, res
    data = res.json()
    assert len(data["results"]) == 2, data
    assert data["total"]["value"] == 2, data
    own = filter_id(data["results"], "b89c677ed30888623500bfbfdb384d3eec259070")[0]
    assert own["schema"] == "Ownership", own
    assert own["properties"]["owner"] == [
        "281d01c426ce39ddf80aa0e46574843c1ba8bfc9"
    ], own

    assert len(own["properties"]["asset"]) == 1, own
    asset = own["properties"]["asset"][0]
    assert asset["schema"] == "Company", asset
    assert asset["properties"]["name"][0] == "Fake Invest GmbH", asset


def test_adjacent_prop_limit():
    """Get the first of two adjacent entities"""
    res = client.get(
        "/entities/281d01c426ce39ddf80aa0e46574843c1ba8bfc9/adjacent/addressEntity?limit=1"
    )
    assert res.status_code == 200, res
    data = res.json()
    assert data["limit"] == 1, data
    assert (
        len(filter_id(data["results"], "582e64e961eaf537d8a80856520d601519a5fb98")) == 1
    ), data
    assert len(data["results"]) == 1, data
    assert data["total"]["value"] == 2, data

    res = client.get(
        "/entities/281d01c426ce39ddf80aa0e46574843c1ba8bfc9/adjacent/ownershipOwner?limit=1"
    )
    assert res.status_code == 200, res
    data = res.json()
    assert len(data["results"]) == 1, data
    assert data["total"]["value"] == 2, data
    assert (
        len(filter_id(data["results"], "b89c677ed30888623500bfbfdb384d3eec259070")) == 1
    ), data

    asset = data["results"][0]["properties"]["asset"][0]
    assert asset["properties"]["name"][0] == "Fake Invest GmbH", asset


def test_adjacent_prop_offset():
    """Get the second of two adjacent entities"""
    res = client.get(
        "/entities/281d01c426ce39ddf80aa0e46574843c1ba8bfc9/adjacent/addressEntity?limit=1&offset=1"
    )
    assert res.status_code == 200, res
    data = res.json()
    assert data["limit"] == 1, data
    assert data["offset"] == 1, data
    assert (
        len(filter_id(data["results"], "012a13cddb445def96357093c4ebb30c3c5ab41d")) == 1
    ), data
    assert len(data["results"]) == 1, data
    assert data["total"]["value"] == 2, data
    res = client.get(
        "/entities/281d01c426ce39ddf80aa0e46574843c1ba8bfc9/adjacent/ownershipOwner?limit=1&offset=1"
    )
    assert res.status_code == 200, res
    data = res.json()

    assert len(data["results"]) == 1, data
    assert data["total"]["value"] == 2, data
    assert (
        len(filter_id(data["results"], "b89c677ed30888623500bbbbbbbbbbbbbbbbbbbb")) == 1
    ), data
    asset = data["results"][0]["properties"]["asset"][0]
    print("asset", asset)
    assert asset["properties"]["name"][0] == "Fake Holding GmbH", asset
