import pytest

from yente import settings
from yente.search.mapping import INDEX_SETTINGS, make_entity_mapping
from yente.data.entity import Entity
from yente.search.indexer import build_indexable_entity_doc


INDEX_MAPPINGS = make_entity_mapping()


@pytest.mark.asyncio
async def test_mappings_copy_to(search_provider):
    """Test that all the mapping and indexing magic works.

    We test that by executing queries on specific fields of the indexed documents."""
    # Create a test index using the same settings as test_search_provider
    temp_index = settings.INDEX_NAME + "-mappings-test"
    try:
        await search_provider.create_index(
            temp_index, mappings=INDEX_MAPPINGS, settings=INDEX_SETTINGS
        )

        # Create a test entity with Vladimir Putin data
        entity = Entity.from_dict(
            {
                "id": "Q7747",
                "schema": "Person",
                "properties": {
                    "name": ["Vladimir Putin"],
                    "citizenship": ["ru"],
                    "topics": ["sanction"],
                },
                "datasets": ["test"],
                "referents": [],
                "first_seen": "2023-01-01T00:00:00",
                "last_seen": "2023-01-01T00:00:00",
                "last_change": "2023-01-01T00:00:00",
            }
        )

        action = {
            "_index": temp_index,
            "_id": entity.id,
            "_source": build_indexable_entity_doc(entity),
        }
        await search_provider.bulk_index([action])
        await search_provider.refresh(temp_index)

        # names is a text field, so need to use match
        search_result = await search_provider.search(
            temp_index, {"bool": {"must": [{"match": {"names": "Vladimir"}}]}}
        )
        assert len(search_result["hits"]["hits"]) == 1, "Failed to match on names"

        # name_parts and name_phonetic are a bit of a special case, we syntesize them in the indexer
        search_result = await search_provider.search(
            temp_index, {"bool": {"must": [{"match": {"name_parts": "Vladimir"}}]}}
        )
        assert len(search_result["hits"]["hits"]) == 1, "Failed to match on name_parts"
        search_result = await search_provider.search(
            temp_index, {"bool": {"must": [{"match": {"name_phonetic": "FLTMR"}}]}}
        )
        assert (
            len(search_result["hits"]["hits"]) == 1
        ), "Failed to match on name_phonetic"

        # Try to match on the countries field, which is a type field that is populated by copy_to from citizenship
        search_result = await search_provider.search(
            temp_index, {"bool": {"must": [{"term": {"countries": "ru"}}]}}
        )
        assert len(search_result["hits"]["hits"]) == 1, "Failed to match on countries"

        # Try to match on the text field, which is a copy_to field that is populated by just about everything
        search_result = await search_provider.search(
            temp_index, {"bool": {"must": [{"term": {"text": "ru"}}]}}
        )
        assert (
            len(search_result["hits"]["hits"]) == 1
        ), "Failed to match country on text"
        search_result = await search_provider.search(
            temp_index, {"bool": {"must": [{"term": {"text": "sanction"}}]}}
        )
        assert len(search_result["hits"]["hits"]) == 1, "Failed to match topics on text"
        search_result = await search_provider.search(
            temp_index, {"bool": {"must": [{"match": {"text": "vladimir"}}]}}
        )
        assert len(search_result["hits"]["hits"]) == 1, "Failed to match name on text"
    finally:
        # Clean up the test index
        await search_provider.delete_index(temp_index)


def test_colliding_prop_names():
    """Test that we can handle multiple properties with the same property name."""
    mapping = make_entity_mapping()
    # Yo dawg, I heard you like properties
    prop_mapping = mapping["properties"]["properties"]["properties"]
    # CallForTenders:authority is an entity, Identification:authority is a string
    assert set(prop_mapping["authority"]["copy_to"]) == set(["text", "entities"])
    assert prop_mapping["authority"]["type"] == "keyword"


@pytest.mark.asyncio
async def test_name_symbols_indexed_person(search_provider):
    """Test that name symbols are indexed correctly for people."""
    entity = Entity.from_dict(
        {
            "id": "Q7747",
            "schema": "Person",
            "properties": {
                "name": ["Vladimir Putin"],
                "citizenship": ["ru"],
                "topics": ["sanction"],
            },
            "datasets": ["test"],
            "referents": [],
            "first_seen": "2023-01-01T00:00:00",
            "last_seen": "2023-01-01T00:00:00",
            "last_change": "2023-01-01T00:00:00",
        }
    )

    doc = build_indexable_entity_doc(entity)
    from pprint import pprint

    pprint(doc)

    assert "NAME:30524893" in doc["name_symbols"]  # Putin


@pytest.mark.asyncio
async def test_name_symbols_indexed_org(search_provider):
    """Test that name symbols are indexed correctly for organizations."""
    entity = Entity.from_dict(
        {
            "id": "Q1234",
            "schema": "Company",
            "properties": {
                "name": ["Gazprom Bank OOO"],
                "topics": ["sanction"],
            },
            "datasets": ["test"],
            "referents": [],
            "first_seen": "2023-01-01T00:00:00",
            "last_seen": "2023-01-01T00:00:00",
            "last_change": "2023-01-01T00:00:00",
        }
    )

    doc = build_indexable_entity_doc(entity)
    from pprint import pprint

    pprint(doc)

    assert "ORGCLS:LLC" in doc["name_symbols"]
    assert "SYMBOL:BANK" in doc["name_symbols"]
