from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from yente.data.common import EntityAdjacentResponse, EntityResponse, TotalSpec
from yente.data.entity import Entity
from yente.provider import get_provider
from yente.routers import search
from yente.util import EntityRedirect


@pytest.fixture
def redirect_client(monkeypatch):
    entity = Entity.from_dict(
        {
            "id": "canonical-person",
            "schema": "Person",
            "properties": {"name": ["Example Person"]},
        }
    )

    async def resolve(_provider, entity_id):
        if entity_id == "old-person":
            raise EntityRedirect("canonical-person")
        if entity_id == entity.id:
            return entity
        return None

    monkeypatch.setattr(search, "get_entity", resolve)
    nested = AsyncMock(
        return_value=(EntityResponse.from_entity(entity), TotalSpec(value=0))
    )
    monkeypatch.setattr(search, "get_nested_entity", nested)
    adjacent = AsyncMock(
        return_value=EntityAdjacentResponse(
            entity=EntityResponse.from_entity(entity), adjacent={}
        )
    )
    monkeypatch.setattr(search, "get_adjacent_entities", adjacent)
    app = FastAPI()
    app.include_router(search.router)
    app.dependency_overrides[get_provider] = lambda: object()
    return TestClient(app), nested


@pytest.mark.parametrize("suffix", ["", "/adjacent", "/adjacent/addressEntity"])
@pytest.mark.parametrize(
    "query", ["", "?limit=2&offset=1&sort=name%3Adesc&sort=id%3Aasc"]
)
def test_redirect_preserves_endpoint_and_query(redirect_client, suffix, query):
    client, _ = redirect_client
    response = client.get(
        f"/entities/old-person{suffix}{query}", follow_redirects=False
    )
    assert response.status_code == 308
    assert response.headers["location"] == f"/entities/canonical-person{suffix}{query}"


def test_follow_redirect_preserves_nested_false(redirect_client):
    client, nested = redirect_client
    response = client.get("/entities/old-person?nested=false")
    assert response.status_code == 200
    assert response.json()["id"] == "canonical-person"
    nested.assert_not_awaited()


def test_follow_redirect_preserves_adjacent_response(redirect_client):
    client, nested = redirect_client
    response = client.get("/entities/old-person/adjacent?limit=2&offset=1")
    assert response.status_code == 200
    assert response.json()["entity"]["id"] == "canonical-person"
    assert response.json()["adjacent"] == {}
    nested.assert_not_awaited()


def test_follow_redirect_preserves_property_pagination(redirect_client):
    client, nested = redirect_client
    response = client.get(
        "/entities/old-person/adjacent/addressEntity?limit=2&offset=1"
    )
    assert response.status_code == 200
    assert response.json()["limit"] == 2
    assert response.json()["offset"] == 1
    assert nested.await_args.args[-2:] == (2, 1)


@pytest.mark.parametrize("suffix", ["", "/adjacent", "/adjacent/addressEntity"])
def test_missing_entity_does_not_redirect(redirect_client, suffix):
    client, _ = redirect_client
    response = client.get(f"/entities/missing-person{suffix}", follow_redirects=False)
    assert response.status_code == 404
    assert "location" not in response.headers


def test_canonical_entity_does_not_redirect(redirect_client):
    client, nested = redirect_client
    response = client.get(
        "/entities/canonical-person?nested=false", follow_redirects=False
    )
    assert response.status_code == 200
    assert response.json()["id"] == "canonical-person"
    assert "location" not in response.headers
    nested.assert_not_awaited()
