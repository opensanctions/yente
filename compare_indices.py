import asyncio
from yente.provider import get_provider
import requests
import csv

LIMIT = 500
PAGE_SIZE = 100
CURRENT_INDEX = "prod-entities"
OLD_INDEX = "debug-2-prod-entities-default-014030804-20250827125412-oud"


async def fetch_entity(provider, entity_id):
    """Fetch a single entity by ID from the provider."""
    query = {
        "bool": {
            "must": [
                {"term": {"_id": entity_id}}
            ]
        }
    }
    
    response = await provider.search(
        index=OLD_INDEX,
        query=query,
        size=1,
    )
    
    if response["hits"]["total"]["value"] > 0:
        return response["hits"]["hits"][0]["_source"]
    else:
        return None


async def query_entities(provider, child_name, offset):
    query = {
        "bool": {
            "must": [
                {"term": {"datasets": child_name}},
                {"range": {"last_change": {"lt": "2025-08-27"}}}
            ]
        }
    }
    return await provider.search(
        index=CURRENT_INDEX,
        query=query,
        size=PAGE_SIZE,
        sort=[{"last_change": {"order": "desc"}}],
        from_=offset,
    )


async def run(children):
    provider = await get_provider()
    comparison_file = open("comparison.csv", "w")
    writer = csv.DictWriter(comparison_file, ["dataset", "id", "current_last_change", "old_last_change"])
    writer.writeheader()

    for child_name in children:
        offset = 0
        print(child_name)
        while offset < LIMIT:
            print(offset)
            response = await query_entities(provider, child_name, offset)
            if response["hits"]["total"]["value"] == 0:
                break
            offset += PAGE_SIZE
            for hit in response["hits"]["hits"]:
                entity = await fetch_entity(provider, hit["_id"])
                
                if entity is None or entity["last_change"] == hit["_source"]["last_change"]:
                    continue

                writer.writerow({
                    "dataset": child_name,
                    "id": hit["_id"],
                    "current_last_change": hit["_source"]["last_change"],
                    "old_last_change": entity["last_change"],
                })
            comparison_file.flush()
    comparison_file.close()
    

if __name__ == "__main__":
    index = requests.get("https://data.opensanctions.org/datasets/latest/index.json").json()
    default = [d for d in index["datasets"] if d["name"] == "default"][0]
    asyncio.run(run(default["children"]))
