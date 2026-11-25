from typing import Any
from unittest import mock

import pytest
import pytest_asyncio
from orjson import dumps
from structlog.testing import capture_logs

from yente import settings
from yente.data.manifest import Manifest
from yente.data.match_v2 import EntityMatchQuery
from yente.routers.match import get_algorithm
from yente.routers.match.v2 import (
    MATCH_EXAMPLES,
    check_query_entity,
    parse_dataset_query,
)
from yente.search.indexer import update_index

from .conftest import build_index_alias_name_for_fixture, client, patch_yente_catalog

# Two datasets with distinct tags, and a `default` collection containing both,
# so that dataset queries can select either, both or neither.
NAME = "Jane Fixture Example"
ADDRESS = "No. 53 Monivong Boulevard, Phnom Penh"
DATASET_ENTITIES: dict[str, list[dict[str, Any]]] = {
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
    for name, entities in DATASET_ENTITIES.items():
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
    datasets.append(
        {"name": "default", "title": "Default", "children": list(DATASET_ENTITIES)}
    )
    monkeypatch.setattr(
        settings, "ENTITY_INDEX", build_index_alias_name_for_fixture("match_v2")
    )
    manifest = Manifest.model_validate({"datasets": datasets})
    async with patch_yente_catalog(manifest):
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
    assert "match" not in data["results"][0]
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
def test_query_echo():
    # A single value is shorthand for a list, and numbers are read as strings.
    query = {
        "schema": "Person",
        "properties": {
            "name": NAME,
            "country": "Russia",
            "birthDate": [1970],
            "weight": [70.50],
        },
    }
    resp = post({"query": query})
    assert resp.status_code == 200, resp.text
    assert resp.json()["query"] == {
        "schema": "Person",
        "properties": {
            "name": [NAME],
            "country": ["ru"],
            "birthDate": ["1970"],
            "weight": ["70.5"],
        },
    }


def person(**properties: Any) -> dict[str, Any]:
    return {"schema": "Person", "properties": {"name": [NAME], **properties}}


@pytest.mark.parametrize(
    "query,locs",
    [
        pytest.param(
            {"schema": "Banana", "properties": {"name": [NAME]}},
            [["body", "query", "schema"]],
            id="unknown-schema",
        ),
        pytest.param(
            {"schema": "Ownership", "properties": {"role": ["owner"]}},
            [["body", "query", "schema"]],
            id="non-matchable-schema",
        ),
        pytest.param(
            {"schema": "LegalEntity", "properties": {"birthDate": ["1970-05-01"]}},
            [["body", "query", "properties", "birthDate"]],
            id="property-not-in-schema",
        ),
        pytest.param(
            person(barfDate=["1970-05-01"]),
            [["body", "query", "properties", "barfDate"]],
            id="unknown-property",
        ),
        pytest.param(
            person(birthDate=["1970-13-28"]),
            [["body", "query", "properties", "birthDate", 0]],
            id="invalid-date",
        ),
        pytest.param(
            person(
                barfDate=["x"], birthDate=["1970", "1970-02-30"], country=["Narnia"]
            ),
            [
                ["body", "query", "properties", "barfDate"],
                ["body", "query", "properties", "birthDate", 1],
                ["body", "query", "properties", "country", 0],
            ],
            id="all-errors-reported",
        ),
        pytest.param(
            person(birthDate=[None]),
            [["body", "query", "properties", "birthDate", 0]],
            id="null-value",
        ),
        pytest.param(
            person(birthDate=None),
            [["body", "query", "properties", "birthDate", 0]],
            id="null-property",
        ),
        pytest.param(
            person(birthDate=[" "]),
            [["body", "query", "properties", "birthDate", 0]],
            id="blank-value",
        ),
        pytest.param(
            person(birthDate=[]),
            [["body", "query", "properties", "birthDate"]],
            id="empty-list",
        ),
        pytest.param(
            person(birthDate=[{"year": 1970}]),
            [["body", "query", "properties", "birthDate", 0]],
            id="object-value",
        ),
        pytest.param(
            person(birthDate=[True]),
            [["body", "query", "properties", "birthDate", 0]],
            id="bool-value",
        ),
    ],
)
def test_query_entity_validation(query, locs):
    resp = post({"query": query})
    assert resp.status_code == 422, resp.text
    assert [error["loc"] for error in resp.json()["detail"]] == locs


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
        pytest.param({"cutoff": 0.5}, ["body", "cutoff"], id="cutoff"),
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
        pytest.param({"datasets": "default -#list.pep"}, ["alpha-person"], id="string"),
    ],
)
@pytest.mark.usefixtures("match_v2_dataset")
def test_dataset_scope(filters, expected):
    assert sorted(result_ids({"query": QUERY_PERSON, "filters": filters})) == expected


@pytest.mark.parametrize(
    "filters,expected",
    [
        pytest.param({}, ["alpha-person"], id="default-dataset"),
        pytest.param({"datasets": ["beta"]}, ["beta-person"], id="request-datasets"),
    ],
)
@pytest.mark.usefixtures("match_v2_dataset")
def test_default_dataset(monkeypatch, filters, expected):
    monkeypatch.setattr(settings, "DEFAULT_DATASET", "alpha")
    assert result_ids({"query": QUERY_PERSON, "filters": filters}) == expected


@pytest.mark.usefixtures("match_v2_dataset")
def test_unknown_default_dataset(monkeypatch):
    monkeypatch.setattr(settings, "DEFAULT_DATASET", "nope")
    resp = post({"query": QUERY_PERSON})
    assert resp.status_code == 422, resp.text


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
def test_threshold():
    # The details of the threshold are tested against score_results. This
    # checks that the request's threshold reaches it, and that only matches
    # are returned.
    # The birth date doesn't match, so the result scores below the default threshold.
    query = {
        "schema": "Person",
        "properties": {"name": [NAME], "birthDate": ["1980-01-01"]},
    }
    body: dict[str, Any] = {"query": query, "filters": {"datasets": ["alpha"]}}
    assert result_ids(body) == []

    resp = post({**body, "threshold": 0.5})
    assert resp.status_code == 200, resp.text
    [result] = resp.json()["results"]

    # A result scoring exactly the threshold is a match, and is returned.
    assert result_ids({**body, "threshold": result["score"]}) == ["alpha-person"]


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


@pytest.mark.parametrize("name", list(MATCH_EXAMPLES))
def test_match_examples(name):
    # The examples work against any index, so check them the way a request is
    # checked before it's run against one.
    match = EntityMatchQuery.model_validate(MATCH_EXAMPLES[name]["value"])
    check_query_entity(match.query)
    if match.filters.datasets is not None:
        parse_dataset_query(match.filters.datasets)
    algorithm = get_algorithm(match.algorithm, match.matcher.config)
    assert set(match.matcher.weights) <= {f.name for f in algorithm.features}


@pytest.mark.usefixtures("match_v2_dataset")
def test_logged_fields():
    # The logged fields are listed in the endpoint's docstring.
    # Take care not to introduce logging that reveals personally
    # identifying information.
    with capture_logs() as logs:
        result_ids({"query": QUERY_PERSON, "filters": {"topics": ["role.pep"]}})
    [event] = [e for e in logs if e["event"] == "/v2/match"]
    assert event == {
        "event": "/v2/match",
        "log_level": "info",
        "action": "match",
        "schema": "Person",
        "results": 1,
        "threshold": settings.SCORE_THRESHOLD,
        "datasets": "default",
        "limit": settings.MATCH_PAGE,
        "algorithm_param": settings.DEFAULT_ALGORITHM,
    }
