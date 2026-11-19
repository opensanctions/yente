from typing import Any
from unittest import mock

import pytest
import pytest_asyncio
from orjson import dumps

from yente import settings
from yente.data.manifest import Manifest
from yente.search.indexer import update_index

from .conftest import build_index_alias_name_for_fixture, client, patch_yente_catalog

# Two datasets with distinct tags, and a `default` collection containing both,
# so that dataset queries can select either, both or neither.
NAME = "Jane Fixture Example"
ADDRESS = "No. 53 Monivong Boulevard, Phnom Penh"
ENTITIES: dict[str, list[dict[str, Any]]] = {
    "alpha": [
        {
            "id": "alpha-person",
            "schema": "Person",
            "referents": ["alpha-old-id"],
            "properties": {
                "name": [NAME],
                "birthDate": ["1970-05-01"],
                "topics": ["sanction"],
            },
        },
        {
            "id": "alpha-company",
            "schema": "Company",
            "properties": {"name": [NAME]},
        },
        {
            "id": "alpha-hotel",
            "schema": "Organization",
            "properties": {"name": ["Phnom Penh Hotel"], "address": [ADDRESS]},
        },
    ],
    "beta": [
        {
            "id": "beta-person",
            "schema": "Person",
            "properties": {
                "name": [NAME],
                "birthDate": ["1970-05-01"],
                "topics": ["role.pep"],
            },
        },
    ],
}
LAST_CHANGE = "2026-01-01T00:00:00"
QUERY_PERSON = {"schema": "Person", "properties": {"name": [NAME]}}


@pytest_asyncio.fixture
async def match_v2_dataset(tmp_path, monkeypatch):
    datasets: list[dict[str, Any]] = []
    tags = {"alpha": ["list.sanction"], "beta": ["list.pep"]}
    for name, entities in ENTITIES.items():
        path = tmp_path / f"{name}.ftm.json"
        path.write_bytes(
            b"\n".join(
                dumps({**e, "datasets": [name], "last_change": LAST_CHANGE})
                for e in entities
            )
        )
        datasets.append(
            {
                "name": name,
                "title": name.title(),
                "tags": tags[name],
                "entities_url": path.as_uri(),
                "load": True,
                "version": "1",
            }
        )
    datasets.append({"name": "default", "title": "Default", "children": list(ENTITIES)})
    monkeypatch.setattr(
        settings, "ENTITY_INDEX", build_index_alias_name_for_fixture("match_v2")
    )
    async with patch_yente_catalog(Manifest.model_validate({"datasets": datasets})):
        await update_index()
        yield


def post(body: dict[str, Any]) -> Any:
    return client.post("/v2/match", json=body)


def result_ids(body: dict[str, Any]) -> list[str]:
    resp = post(body)
    assert resp.status_code == 200, resp.text
    return [r["id"] for r in resp.json()["results"]]


@pytest.mark.usefixtures("match_v2_dataset")
def test_match():
    body = {"query": QUERY_PERSON, "filters": {"datasets": ["alpha"]}}
    resp = post(body)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert set(data.keys()) == {"query", "results", "total"}
    assert [r["id"] for r in data["results"]] == ["alpha-person"]
    assert data["results"][0]["match"] is True
    assert data["total"] == {"value": 1, "relation": "eq"}


@pytest.mark.usefixtures("match_v2_dataset")
def test_matcher_config():
    body = {
        "query": QUERY_PERSON,
        "algorithm": "logic-v2",
        "matcher": {"config": {"nm_number_mismatch": 0.4}},
    }
    with mock.patch(
        "nomenklatura.matching.logic_v2.model.LogicV2.compare"
    ) as mock_compare:
        mock_compare.return_value = mock.MagicMock()
        mock_compare.return_value.score = 0.8
        mock_compare.return_value.explanations = {}
        resp = post(body)
        assert mock_compare.called
        _, call_kwargs = mock_compare.call_args
        assert call_kwargs["config"].config.get("nm_number_mismatch") == 0.4
    assert resp.status_code == 200, resp.text

    body["matcher"] = {"config": {"invalid_option": 0.4}}
    resp = post(body)
    assert resp.status_code == 400, resp.text
    assert "invalid_option" in resp.json()["detail"]


@pytest.mark.usefixtures("match_v2_dataset")
def test_matcher_weights():
    # address_match has a default weight of zero, so an address alone only
    # produces a match when the request gives it a weight.
    query = {"schema": "Organization", "properties": {"address": [ADDRESS]}}
    assert result_ids({"query": query}) == []
    body = {"query": query, "matcher": {"weights": {"address_match": 1.0}}}
    assert result_ids(body) == ["alpha-hotel"]


@pytest.mark.usefixtures("match_v2_dataset")
def test_unknown_algorithm():
    resp = post({"query": QUERY_PERSON, "algorithm": "nope"})
    assert resp.status_code == 400, resp.text
    assert "Invalid algorithm" in resp.json()["detail"]


@pytest.mark.usefixtures("match_v2_dataset")
def test_unknown_schema():
    resp = post({"query": {"schema": "Banana", "properties": {"name": [NAME]}}})
    assert resp.status_code == 400, resp.text
    assert "Cannot parse example entity" in resp.json()["detail"]


@pytest.mark.usefixtures("match_v2_dataset")
def test_query_echo():
    query = {
        "id": "my-id",
        "schema": "Person",
        "properties": {"name": [NAME], "country": "Russia"},
    }
    resp = post({"query": query})
    assert resp.status_code == 200, resp.text
    echoed = resp.json()["query"]
    assert echoed["id"] == "my-id"
    assert echoed["schema"] == "Person"
    assert echoed["properties"]["country"] == ["ru"]


@pytest.mark.parametrize(
    "body",
    [
        pytest.param({"filters": {"datasets": "alpha"}}, id="datasets-string"),
        pytest.param({"limit": settings.MAX_MATCHES + 1}, id="limit"),
        pytest.param(
            {"filters": {"exclude_entities": [f"id-{i}" for i in range(51)]}},
            id="exclude-entities",
        ),
        pytest.param({"filters": {"changed_since": "yesterday"}}, id="changed-since"),
        pytest.param({"query": None}, id="no-query"),
    ],
)
def test_request_validation(body):
    resp = post({"query": QUERY_PERSON, **body})
    assert resp.status_code == 422, resp.text


@pytest.mark.parametrize(
    "datasets,detail",
    [
        pytest.param(["nope"], "Unknown dataset: 'nope'", id="unknown-dataset"),
        pytest.param({"xor": ["alpha"]}, "Unknown operator: 'xor'", id="operator"),
    ],
)
@pytest.mark.usefixtures("match_v2_dataset")
def test_invalid_dataset_query(datasets, detail):
    resp = post({"query": QUERY_PERSON, "filters": {"datasets": datasets}})
    assert resp.status_code == 400, resp.text
    assert detail in resp.json()["detail"]


# We don't have to test the query language exhaustively. Just that the logic is invoked.
@pytest.mark.parametrize(
    "filters,expected",
    [
        pytest.param({}, ["alpha-person", "beta-person"], id="default"),
        pytest.param(
            {"datasets": {"or": ["beta", "#list.sanction"]}},
            ["alpha-person", "beta-person"],
            id="dataset",
        ),
        pytest.param({"datasets": ["#list.pep"]}, ["beta-person"], id="tag"),
    ],
)
@pytest.mark.usefixtures("match_v2_dataset")
def test_dataset_scope(filters, expected):
    assert sorted(result_ids({"query": QUERY_PERSON, "filters": filters})) == expected


@pytest.mark.usefixtures("match_v2_dataset")
def test_exclude_entities():
    filters: dict[str, Any] = {"datasets": ["alpha"]}
    assert result_ids({"query": QUERY_PERSON, "filters": filters}) == ["alpha-person"]
    for excluded in ["alpha-person", "alpha-old-id"]:
        filters["exclude_entities"] = [excluded]
        assert result_ids({"query": QUERY_PERSON, "filters": filters}) == []


@pytest.mark.usefixtures("match_v2_dataset")
def test_topics():
    body = {"query": QUERY_PERSON, "filters": {"topics": ["role.pep"]}}
    assert result_ids(body) == ["beta-person"]


@pytest.mark.usefixtures("match_v2_dataset")
def test_exclude_schemata():
    query = {"schema": "LegalEntity", "properties": {"name": [NAME]}}
    filters: dict[str, Any] = {"datasets": ["alpha"]}
    ids = result_ids({"query": query, "filters": filters})
    assert sorted(ids) == ["alpha-company", "alpha-person"]
    filters["exclude_schemata"] = ["Company"]
    assert result_ids({"query": query, "filters": filters}) == ["alpha-person"]


@pytest.mark.parametrize(
    "changed_since,expected",
    [
        pytest.param("2025-06-01", ["alpha-person", "beta-person"], id="before"),
        pytest.param("2026-06-01", [], id="after"),
    ],
)
@pytest.mark.usefixtures("match_v2_dataset")
def test_changed_since(changed_since, expected):
    body = {"query": QUERY_PERSON, "filters": {"changed_since": changed_since}}
    assert sorted(result_ids(body)) == expected


@pytest.mark.usefixtures("match_v2_dataset")
def test_threshold_and_cutoff():
    # The details of threshold and cutoff are tested against score_results. This
    # checks that the request values reach it, and that the cutoff is capped.
    body: dict[str, Any] = {"query": QUERY_PERSON, "filters": {"datasets": ["alpha"]}}
    resp = post({**body, "threshold": 1.01, "cutoff": 0.0})
    assert resp.status_code == 200, resp.text
    assert [r["match"] for r in resp.json()["results"]] == [False]

    # A cutoff above the threshold is lowered to the threshold.
    resp = post({**body, "threshold": 0.0, "cutoff": 1.01})
    assert resp.status_code == 200, resp.text
    assert [r["match"] for r in resp.json()["results"]] == [True]


@pytest.mark.usefixtures("match_v2_dataset")
def test_metric_attributes():
    import yente.routers.match as match_router

    with mock.patch.object(
        match_router, "search_entities", wraps=match_router.search_entities
    ) as spy:
        resp = post({"query": QUERY_PERSON})
        assert resp.status_code == 200, resp.text
        assert spy.call_args.kwargs["metric_attributes"] == {"endpoint": "match_v2"}

        resp = client.post("/match/default", json={"queries": {"q": QUERY_PERSON}})
        assert resp.status_code == 200, resp.text
        assert spy.call_args.kwargs["metric_attributes"] == {"endpoint": "match"}
