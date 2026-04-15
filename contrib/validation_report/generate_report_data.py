#!/usr/bin/env python3
"""Validation report: compare logic-v1 vs logic-v2 against fixture JSONs via the yente HTTP API."""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import click
import httpx
import orjson

log = logging.getLogger(__name__)

ALGORITHMS = ["logic-v1", "logic-v2"]
BATCH_SIZE = 100
FIXTURES_DIR = Path(__file__).parent / "build" / "fixtures"


@dataclass
class Fixture:
    name: str
    entities: list[dict[str, Any]]  # FTM entities: {id, schema, properties}


def read_fixture(path: Path) -> Fixture:
    data = orjson.loads(path.read_bytes())
    return Fixture(name=data["name"], entities=data["data"])


def entity_to_query(entity: dict[str, Any]) -> dict[str, Any]:
    """Build a yente match query from an FTM entity, excluding the id."""
    return {"schema": entity["schema"], "properties": entity["properties"]}


def run_fixture(
    client: httpx.Client,
    fixture: Fixture,
    algorithm: str,
    base_url: str,
    dataset: str,
) -> list[dict[str, Any]]:
    """Returns a list of result dicts, one per entity."""
    results: list[dict[str, Any]] = []
    entities = fixture.entities
    total = len(entities)

    for batch_start in range(0, total, BATCH_SIZE):
        batch = entities[batch_start : batch_start + BATCH_SIZE]
        batch_end = min(batch_start + BATCH_SIZE, total)
        log.info("[%s] %s: %d/%d", algorithm, fixture.name, batch_end, total)
        queries = {f"q{i}": entity_to_query(e) for i, e in enumerate(batch)}
        resp = client.post(
            f"{base_url}/match/{dataset}",
            json={"queries": queries},
            params={"algorithm": algorithm, "limit": 5},
        )
        resp.raise_for_status()
        data = resp.json()
        for i, key in enumerate(queries):
            entity = batch[i]
            hits = data["responses"][key].get("results", [])
            if hits:
                top = hits[0]
                result: dict[str, Any] = {"score": top["score"], "match": top["match"]}
            else:
                result = {"score": 0.0, "match": False}

            # Check if expected entity ID appears in the top results
            expected_id = entity["id"]
            hit_ids = [h["id"] for h in hits]
            result["expected_id_found"] = expected_id in hit_ids
            if expected_id in hit_ids:
                idx = hit_ids.index(expected_id)
                result["expected_id_rank"] = idx + 1
                result["expected_id_score"] = hits[idx]["score"]

            results.append(result)

    return results


@click.command()
@click.option("--dataset", default="default", show_default=True)
@click.option("--base-url", default="http://localhost:8000", show_default=True)
@click.option(
    "--output",
    default="build/report_data.json",
    show_default=True,
    type=click.Path(),
    help="Path to write the JSON report.",
)
def main(dataset: str, base_url: str, output: str) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    fixtures = [read_fixture(p) for p in sorted(FIXTURES_DIR.glob("*.json"))]
    if not fixtures:
        log.error(
            "No fixture JSONs found in %s. Run generate_fixtures.py first.",
            FIXTURES_DIR,
        )
        raise SystemExit(1)

    report: dict[str, dict[str, list[dict[str, Any]]]] = {}

    with httpx.Client(timeout=60.0) as client:
        for fixture in fixtures:
            log.info("fixture: %s (%d entities)", fixture.name, len(fixture.entities))
            report[fixture.name] = {}
            for algo in ALGORITHMS:
                report[fixture.name][algo] = run_fixture(
                    client, fixture, algo, base_url, dataset
                )

    output_path = Path(output)
    output_path.write_bytes(orjson.dumps(report, option=orjson.OPT_INDENT_2))
    log.info("report written to %s", output_path)


if __name__ == "__main__":
    main()
