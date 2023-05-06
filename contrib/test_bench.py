import asyncio
from pprint import pprint
from nomenklatura.matching import get_algorithm

from yente.data.entity import Entity
from yente.data.common import EntityExample
from yente.search.base import close_es
from yente.search.queries import entity_query
from yente.search.search import search_entities, result_entities
from yente.data.entity import Entity
from yente.scoring import score_results
from yente.routers.util import get_dataset

LIMIT = 10
EXAMPLE = {
    "schema": "Person",
    "properties": {
        "name": ["Angel RODRIGUEZ"],
    },
}


async def test_example():
    ds = await get_dataset("default")
    example = EntityExample.parse_obj(EXAMPLE)
    entity = Entity.from_example(example)
    query = entity_query(ds, entity)
    pprint(query)
    resp = await search_entities(query, limit=LIMIT * 10)

    ents = result_entities(resp)
    print("RAW RESULTS:")
    for ent in result_entities(resp):
        print(ent.id, ent.caption, ent.schema.name)

    algorithm = get_algorithm("name-based")
    scored = score_results(
        algorithm,
        entity,
        ents,
        threshold=0.7,
        cutoff=0.2,
        limit=LIMIT,
    )

    print("\n\nSCORED RESULTS:")
    for res in scored:
        print(res.id, res.caption, res.schema_, res.score)

    await close_es()


asyncio.run(test_example())
