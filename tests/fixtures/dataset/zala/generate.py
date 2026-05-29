"""
Generate the zala test fixture dataset from a running local yente instance.

This script captures a realistic sanctions cluster — Alexander Zakharov
(NK-aU5ybkbRFJucf8YMwsJvDw) and LLC CST / Zala Aero (NK-abdzbEBkqyT29GyREbiURZ),
the Russian drone manufacturer — along with all their adjacent entities.

Usage:
    python generate.py [--yente-url http://localhost:8000]

The two output files are committed to the repo so tests don't need a live yente:
  - entities.ftm.json   flat FTM entities, one JSON object per line (JSONL)
  - index.json          minimal dataset index consumed by yente's catalog loader

Re-run this script whenever you want to refresh the fixture from a newer dataset.
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

# Seed entity IDs — the two entities this fixture is built around.
SEED_IDS = [
    "NK-aU5ybkbRFJucf8YMwsJvDw",  # Alexander Vyacheslavovich Zakharov (Person)
    "NK-abdzbEBkqyT29GyREbiURZ",  # LLC CST / Zala Aero (Company)
]

DATASET_NAME = "zala"
DATASET_TITLE = "Zala Aero Sanctions Test Dataset"

HERE = Path(__file__).parent


def collect_nested_ids(obj: Any, ids: set[str]) -> None:
    """Recursively walk a nested entity response and collect all entity IDs.

    The yente /entities/<id>?nested=true response embeds adjacent entities
    directly inside property values instead of referencing them by ID only.
    Any dict with both "id" and "schema" keys is a full entity object —
    these are the adjacent entities we want to capture.

    This covers:
      - outbound references expanded inline (e.g. addressEntity → Address)
      - inbound "stub" properties (e.g. sanctions → [Sanction, ...])
      - interstitial entities with double nesting (e.g. Ownership referencing
        both owner and asset as nested entities)
    """
    if isinstance(obj, dict):
        if "id" in obj and "schema" in obj:
            ids.add(obj["id"])
        for value in obj.values():
            collect_nested_ids(value, ids)
    elif isinstance(obj, list):
        for item in obj:
            collect_nested_ids(item, ids)


def fetch_entity_nested(client: httpx.Client, base_url: str, entity_id: str) -> dict:
    """Fetch an entity with nested=true for ID discovery.

    We use nested=true here only to discover adjacent entity IDs — the nested
    response embeds full entity objects in property values, making it easy to
    walk and collect every related entity without knowing the schema up front.
    """
    resp = client.get(f"{base_url}/entities/{entity_id}", params={"nested": "true"})
    resp.raise_for_status()
    return resp.json()


def fetch_entity_flat(client: httpx.Client, base_url: str, entity_id: str) -> dict:
    """Fetch an entity with nested=false for the actual fixture data.

    We use nested=false for the stored fixture because entities.ftm.json is
    a flat format — each entity is self-contained with scalar property values
    and entity IDs for cross-references (not embedded objects). This matches
    what yente expects when indexing from a bulk export file.
    """
    resp = client.get(f"{base_url}/entities/{entity_id}", params={"nested": "false"})
    resp.raise_for_status()
    return resp.json()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--yente-url",
        default="http://localhost:8000",
        help="Base URL of the yente instance to fetch from (default: http://localhost:8000)",
    )
    args = parser.parse_args()
    base_url = args.yente_url.rstrip("/")

    print(f"Fetching from {base_url}")

    all_ids: set[str] = set()

    with httpx.Client(timeout=30) as client:
        # Phase 1: discover all adjacent entity IDs by fetching nested responses
        # for each seed entity. The nested format embeds related entities inline,
        # so we can collect every ID in one recursive walk.
        print("Discovering adjacent entity IDs from nested responses...")
        for seed_id in SEED_IDS:
            print(f"  seed: {seed_id}")
            nested = fetch_entity_nested(client, base_url, seed_id)
            collect_nested_ids(nested, all_ids)

        print(f"Found {len(all_ids)} unique entity IDs")

        # Phase 2: fetch the flat representation for every entity we found.
        # The flat format is what goes into entities.ftm.json.
        print("Fetching flat entities...")
        entities: list[dict] = []
        for entity_id in sorted(all_ids):
            entity = fetch_entity_flat(client, base_url, entity_id)
            entities.append(entity)
            print(f"  {entity['schema']:16s} {entity_id}  {entity.get('caption', '')}")

    # Write entities.ftm.json — JSONL, one entity per line, no trailing newline
    # on the last line. This matches the format yente's indexer expects.
    entities_path = HERE / "entities.ftm.json"
    with entities_path.open("w") as fh:
        for entity in entities:
            fh.write(json.dumps(entity, ensure_ascii=False) + "\n")
    print(f"\nWrote {len(entities)} entities to {entities_path}")

    # Derive timestamps from the entity data for the index metadata.
    updated_at = max(
        (e["last_seen"] for e in entities if e.get("last_seen")),
        default=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
    )
    last_change = max(
        (e["last_change"] for e in entities if e.get("last_change")),
        default=updated_at,
    )

    # Write index.json — minimal dataset-level index consumed by yente's catalog
    # loader. Only the fields yente actually reads are included; counts,
    # schemata, properties, publisher etc. are omitted intentionally.
    index = {
        "name": DATASET_NAME,
        "title": DATASET_TITLE,
        "updated_at": updated_at,
        "last_change": last_change,
        "resources": [
            {
                "name": "entities.ftm.json",
                "url": f"https://data.opensanctions.org/datasets/latest/{DATASET_NAME}/entities.ftm.json",
                "mime_type": "application/json+ftm",
                "mime_type_label": "FollowTheMoney Entities",
                "title": "FollowTheMoney entities",
                "path": "entities.ftm.json",
            }
        ],
    }
    index_path = HERE / "index.json"
    with index_path.open("w") as fh:
        json.dump(index, fh, indent=2, ensure_ascii=False)
        fh.write("\n")
    print(f"Wrote index to {index_path}")


if __name__ == "__main__":
    main()
