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
    query = {"schema": "Person", "properties": {"name": [NAME], "country": "Russia"}}
    resp = post({"query": query})
    assert resp.status_code == 200, resp.text
    assert resp.json()["query"] == {
        "schema": "Person",
        "properties": {"name": [NAME], "country": ["ru"]},
    }


@pytest.mark.parametrize(
    "body,loc",
    [
        pytest.param({"query": None}, ["body", "query"], id="no-query"),
        pytest.param(
            {"query": {"properties": {"name": [NAME]}}},
            ["body", "query", "schema"],
            id="no-schema",
        ),
        pytest.param(
            {"query": {"schema": "", "properties": {"name": [NAME]}}},
            ["body", "query", "schema"],
            id="empty-schema",
        ),
        pytest.param(
            {"query": {"schema": "Person"}},
            ["body", "query", "properties"],
            id="no-properties",
        ),
        pytest.param(
            {"query": {"schema": "Person", "properties": {}}},
            ["body", "query", "properties"],
            id="empty-properties",
        ),
        pytest.param(
            {"query": {**QUERY_PERSON, "id": "my-id"}},
            ["body", "query", "id"],
            id="query-id",
        ),
        pytest.param({"queries": {}}, ["body", "queries"], id="queries"),
        pytest.param(
            {"include_dataset": ["alpha"]}, ["body", "include_dataset"], id="toplevel"
        ),
        pytest.param(
            {"filters": {"exclude_dataset": ["alpha"]}},
            ["body", "filters", "exclude_dataset"],
            id="filters",
        ),
        pytest.param(
            {"matcher": {"weight": {}}}, ["body", "matcher", "weight"], id="matcher"
        ),
        pytest.param({"threshold": 70}, ["body", "threshold"], id="threshold-above"),
        pytest.param({"threshold": -0.1}, ["body", "threshold"], id="threshold-below"),
        pytest.param({"cutoff": 1.5}, ["body", "cutoff"], id="cutoff-above"),
        pytest.param(
            {"threshold": 0.5, "cutoff": 0.6}, ["body"], id="cutoff-above-threshold"
        ),
        pytest.param({"threshold": 0.5}, ["body"], id="default-cutoff-above-threshold"),
        pytest.param(
            {"limit": settings.MAX_MATCHES + 1}, ["body", "limit"], id="limit"
        ),
        pytest.param(
            {"filters": {"exclude_entities": [f"id-{i}" for i in range(51)]}},
            ["body", "filters", "exclude_entities"],
            id="exclude-entities",
        ),
        pytest.param(
            {"filters": {"changed_since": "yesterday"}},
            ["body", "filters", "changed_since"],
            id="changed-since",
        ),
    ],
)
def test_request_validation(body, loc):
    resp = post({"query": QUERY_PERSON, **body})
    assert resp.status_code == 422, resp.text
    assert [error["loc"] for error in resp.json()["detail"]] == [loc]


def test_query_string_rejected():
    resp = client.post(
        "/v2/match", json={"query": QUERY_PERSON}, params={"threshold": 0.5}
    )
    assert resp.status_code == 422, resp.text
    assert [error["loc"] for error in resp.json()["detail"]] == [["query", "threshold"]]


@pytest.mark.parametrize(
    "datasets,detail",
    [
        pytest.param({"xor": ["alpha"]}, "Unknown operator: 'xor'", id="operator"),
        pytest.param([], "Empty array in query", id="empty-list"),
        pytest.param("alpha|", "Expected identifier", id="string-syntax"),
        pytest.param("", "Empty query string", id="string-empty"),
    ],
)
@pytest.mark.usefixtures("match_v2_dataset")
def test_malformed_dataset_query(datasets, detail):
    resp = post({"query": QUERY_PERSON, "filters": {"datasets": datasets}})
    assert resp.status_code == 400, resp.text
    assert detail in resp.json()["detail"]


@pytest.mark.parametrize(
    "datasets",
    [
        pytest.param(["alpha", "nope"], id="structured"),
        pytest.param("alpha|nope", id="string"),
    ],
)
@pytest.mark.usefixtures("match_v2_dataset")
def test_unknown_dataset(datasets):
    resp = post({"query": QUERY_PERSON, "filters": {"datasets": datasets}})
    assert resp.status_code == 422, resp.text
    [error] = resp.json()["detail"]
    assert error["loc"] == ["body", "filters", "datasets"]
    assert error["msg"] == "Unknown dataset: 'nope'"


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
        pytest.param({"datasets": "default-#list.pep"}, ["alpha-person"], id="string"),
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
    # checks that the request values reach it.
    # The birth date doesn't match, so the result scores below the default threshold.
    query = {
        "schema": "Person",
        "properties": {"name": [NAME], "birthDate": ["1980-01-01"]},
    }
    body: dict[str, Any] = {"query": query, "filters": {"datasets": ["alpha"]}}
    assert result_ids(body) == []

    resp = post({**body, "threshold": 1.0, "cutoff": 0.0})
    assert resp.status_code == 200, resp.text
    assert [r["match"] for r in resp.json()["results"]] == [False]

    resp = post({**body, "threshold": 0.5, "cutoff": 0.5})
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
