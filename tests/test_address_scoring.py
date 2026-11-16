import pytest
import pytest_asyncio
from orjson import dumps

from yente import settings
from yente.data.common import EntityExample
from yente.data.entity import Entity
from yente.data.manifest import Manifest
from yente.provider import with_provider
from yente.routers.util import get_dataset
from yente.search.indexer import update_index
from yente.search.queries import Operator, entity_query
from yente.search.search import result_entities, search_entities

from .conftest import build_index_alias_name_for_fixture, patch_yente_catalog

# These tests check the structure of the address boosts in the candidate query:
# how an address's score grows with its length and specificity, relative to a
# name match. With three documents every term has nearly the same IDF, so they
# say nothing about how the boosts are calibrated against real term frequencies.

DENSE_ADDRESS = (
    "127055, Російська Федерація, м. Москва, вул. Бутирський Вал, буд. 68/70 "
    "(будова 1, прим. І, поверх 5, кімн. 7)"
)
VAGUE_ADDRESS = "Moscow, Russia"
ENTITIES = [
    {
        "id": "target",
        "schema": "Organization",
        "properties": {
            "name": ["Phnom Penh Hotel"],
            "address": ["No. 53 Monivong Boulevard, Phnom Penh"],
        },
    },
    {
        "id": "dense",
        "schema": "Company",
        "properties": {"name": ["Railcraft Service"], "address": [DENSE_ADDRESS]},
    },
    {
        "id": "vague",
        "schema": "Company",
        "properties": {"name": ["Reksoft"], "address": [VAGUE_ADDRESS]},
    },
]


@pytest_asyncio.fixture
async def address_scoring_dataset(tmp_path, monkeypatch):
    path = tmp_path / "entities.ftm.json"
    path.write_bytes(
        b"\n".join(dumps({**e, "datasets": ["address_scoring"]}) for e in ENTITIES)
    )
    manifest = Manifest.model_validate(
        {
            "datasets": [
                {
                    "name": "address_scoring",
                    "title": "Address scoring",
                    "entities_url": path.as_uri(),
                    "load": True,
                    "version": "1",
                }
            ]
        }
    )
    monkeypatch.setattr(
        settings,
        "ENTITY_INDEX",
        build_index_alias_name_for_fixture("address_scoring"),
    )
    async with patch_yente_catalog(manifest):
        await update_index()
        yield


async def scores(properties: dict[str, list[str]]) -> dict[str, float]:
    """Candidate query scores by entity ID, for a LegalEntity query."""
    dataset = await get_dataset("address_scoring")
    example = EntityExample(schema="LegalEntity", properties=properties)
    query = entity_query(dataset, Entity.from_example(example), filter_op=Operator.OR)
    async with with_provider() as provider:
        response = await search_entities(provider, query, limit=10)
    return {entity.id: score for entity, score in result_entities(response)}


@pytest.mark.asyncio
@pytest.mark.usefixtures("address_scoring_dataset")
async def test_address_does_not_outweigh_name():
    # The full address of another entity must not outscore a name match.
    s = await scores({"name": ["Phnom Penh Hotel"], "address": [DENSE_ADDRESS]})
    assert s["target"] > s["dense"]

    s = await scores({"name": ["Phnom Penh Hotel"], "address": [VAGUE_ADDRESS]})
    assert s["target"] > s["vague"]
