from .conftest import client


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

    # for fam in props["familyPerson"]:
    #     famprops = fam["properties"]
    #     assert len(famprops["relative"]) > 0, famprops
    #     for rel in famprops["relative"]:
    #         if isinstance(rel, dict):
    #             assert rel["id"] != data["id"], rel
    #         else:
    #             assert rel != data["id"], rel
    #     for rel in famprops.get("relative", []):
    #         if isinstance(rel, dict):
    #             assert rel["id"] != data["id"], rel
    #         else:
    #             assert rel != data["id"], rel
