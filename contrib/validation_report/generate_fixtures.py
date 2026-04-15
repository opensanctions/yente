#!/usr/bin/env python3
"""Download OpenSanctions FTM data and generate deterministic fixture JSON files."""

import copy
import hashlib
import logging
import random
from pathlib import Path
from typing import Any

import click
import httpx
import orjson

from treatments import apply_treatment

log = logging.getLogger(__name__)

BUILD_DIR = Path(__file__).parent / "build" / "fixtures"

FIXTURES: list[dict[str, Any]] = [
    {
        "output_filename": "positives_un_treated.json",
        "dataset": "un_sc_sanctions",
        "treatment": True,
        "description": (
            "Generated from the `un_sc_sanctions` dataset, treated version: "
            "minor typos and name reshuffles applied. These are true-positives."
        ),
    },
    {
        "output_filename": "positives_un_untreated.json",
        "dataset": "un_sc_sanctions",
        "treatment": False,
        "description": "Generated from the `un_sc_sanctions`. These are true-positives.",
    },
    {
        "output_filename": "positives_us_congress_untreated.json",
        "dataset": "us_congress",
        "treatment": False,
        "description": "Generated from the `us_congress` dataset. These are true-positives.",
    },
]


def parse_ftm_entity(line: bytes) -> dict[str, Any] | None:
    """Parse a raw FTM JSON line into a slim entity. Returns None if not a Person."""
    entity = orjson.loads(line)
    if entity.get("schema") != "Person":
        return None
    props = entity.get("properties", {})
    slim_props: dict[str, list[str]] = {}
    for key in (
        "name",
        "firstName",
        "secondName",
        "lastName",
        "birthDate",
        "birthPlace",
        "nationality",
        "gender",
    ):
        values = props.get(key)
        if values:
            slim_props[key] = [values[0]]
    if not slim_props.get("name"):
        return None
    return {
        "id": entity["id"],
        "schema": "Person",
        "properties": slim_props,
    }


def fetch_dataset_version(client: httpx.Client, dataset: str) -> str:
    url = f"https://data.opensanctions.org/datasets/{dataset}/latest/index.json"
    resp = client.get(url)
    resp.raise_for_status()
    return str(resp.json()["version"])


def download_ftm_entities(client: httpx.Client, dataset: str) -> bytes:
    url = f"https://data.opensanctions.org/datasets/latest/{dataset}/entities.ftm.json"
    log.info("Downloading %s ...", url)
    resp = client.get(url, follow_redirects=True)
    resp.raise_for_status()
    return resp.content


@click.command()
def main() -> None:
    """Download OpenSanctions FTM data and generate fixture JSON files in build/fixtures/."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    BUILD_DIR.mkdir(parents=True, exist_ok=True)

    # Group fixtures by dataset to avoid duplicate downloads
    by_dataset: dict[str, list[dict[str, Any]]] = {}
    for fixture_config in FIXTURES:
        by_dataset.setdefault(fixture_config["dataset"], []).append(fixture_config)

    with httpx.Client(timeout=300.0) as client:
        for dataset, fixture_configs in by_dataset.items():
            log.info("Fetching version for dataset: %s", dataset)
            version = fetch_dataset_version(client, dataset)
            log.info("Dataset %s version: %s", dataset, version)

            raw_bytes = download_ftm_entities(client, dataset)
            entities: list[dict[str, Any]] = []
            for line in raw_bytes.splitlines():
                line = line.strip()
                if not line:
                    continue
                parsed = parse_ftm_entity(line)
                if parsed is not None:
                    entities.append(parsed)
            log.info("Parsed %d Person entities from %s", len(entities), dataset)

            for fixture_config in fixture_configs:
                fixture_entities = copy.deepcopy(entities)

                if fixture_config["treatment"]:
                    for entity in fixture_entities:
                        seed = int(
                            hashlib.sha256(entity["id"].encode()).hexdigest(), 16
                        ) % (2**32)
                        rng = random.Random(seed)
                        apply_treatment(entity, rng)

                output: dict[str, Any] = {
                    "name": fixture_config["output_filename"],
                    "description": fixture_config["description"],
                    "generated_from_version": version,
                    "generated_from_dataset": dataset,
                    "data": fixture_entities,
                }

                output_path = BUILD_DIR / fixture_config["output_filename"]
                output_path.write_bytes(
                    orjson.dumps(output, option=orjson.OPT_INDENT_2)
                )
                log.info("Wrote %s (%d records)", output_path, len(fixture_entities))


if __name__ == "__main__":
    main()
