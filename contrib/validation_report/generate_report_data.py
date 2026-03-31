#!/usr/bin/env python3
"""Validation report: compare logic-v1 vs logic-v2 against fixture CSVs via the yente HTTP API."""

import csv
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import click
import httpx

log = logging.getLogger(__name__)

ALGORITHMS = ["logic-v1", "logic-v2"]
BATCH_SIZE = 100
FIXTURES_DIR = Path(__file__).parent / "fixtures"


@dataclass
class PersonRecord:
    full_name: str
    first_name: str
    middle_name: str | None
    last_name: str
    gender: str
    date_of_birth: str
    place_of_birth: str
    nationality: str


def read_person_csv(path: Path) -> list[PersonRecord]:
    persons: list[PersonRecord] = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            persons.append(
                PersonRecord(
                    full_name=row.get("full_name", ""),
                    first_name=row.get("first_name", ""),
                    middle_name=row.get("middle_name") or None,
                    last_name=row.get("last_name", ""),
                    gender=row.get("gender", ""),
                    date_of_birth=row.get("date_of_birth", ""),
                    place_of_birth=row.get("place_of_birth", ""),
                    nationality=row.get("nationality", ""),
                )
            )
    return persons


def person_to_query(person: PersonRecord) -> dict[str, Any]:
    props: dict[str, list[str]] = {}
    if person.full_name:
        props["name"] = [person.full_name]
    if person.first_name:
        props["firstName"] = [person.first_name]
    if person.middle_name:
        props["middleName"] = [person.middle_name]
    if person.last_name:
        props["lastName"] = [person.last_name]
    if person.date_of_birth:
        props["birthDate"] = [person.date_of_birth]
    if person.nationality:
        props["nationality"] = [person.nationality]
    if person.place_of_birth:
        props["birthPlace"] = [person.place_of_birth]
    if person.gender:
        props["gender"] = [person.gender]
    return {"schema": "Person", "properties": props}


def run_fixture(
    client: httpx.Client,
    persons: list[PersonRecord],
    algorithm: str,
    base_url: str,
    dataset: str,
    fixture_name: str,
) -> list[dict[str, Any]]:
    """Returns a list of {score, match} dicts, one per person (top result only)."""
    results: list[dict[str, Any]] = []
    total_persons = len(persons)

    for batch_start in range(0, total_persons, BATCH_SIZE):
        batch = persons[batch_start : batch_start + BATCH_SIZE]
        batch_end = min(batch_start + BATCH_SIZE, total_persons)
        log.info("[%s] %s: %d/%d", algorithm, fixture_name, batch_end, total_persons)
        queries = {f"q{i}": person_to_query(p) for i, p in enumerate(batch)}
        resp = client.post(
            f"{base_url}/match/{dataset}",
            json={"queries": queries},
            params={"algorithm": algorithm, "limit": 5},
        )
        resp.raise_for_status()
        data = resp.json()
        for key in queries:
            hits = data["responses"][key].get("results", [])
            if hits:
                top = hits[0]
                results.append({"score": top["score"], "match": top["match"]})
            else:
                results.append({"score": 0.0, "match": False})

    return results


@click.command()
@click.option("--dataset", default="default", show_default=True)
@click.option("--base-url", default="http://localhost:8000", show_default=True)
@click.option(
    "--output",
    default="report.json",
    show_default=True,
    type=click.Path(),
    help="Path to write the JSON report.",
)
def main(dataset: str, base_url: str, output: str) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    fixtures = sorted(FIXTURES_DIR.glob("*.csv"))
    if not fixtures:
        log.error("No fixture CSVs found in %s", FIXTURES_DIR)
        raise SystemExit(1)

    report: dict[str, dict[str, list[dict[str, Any]]]] = {}

    with httpx.Client(timeout=60.0) as client:
        for fixture_path in fixtures:
            persons = read_person_csv(fixture_path)
            log.info("fixture: %s (%d persons)", fixture_path.name, len(persons))
            report[fixture_path.name] = {}
            for algo in ALGORITHMS:
                report[fixture_path.name][algo] = run_fixture(
                    client, persons, algo, base_url, dataset, fixture_path.name
                )

    output_path = Path(output)
    output_path.write_text(json.dumps(report, indent=2))
    log.info("report written to %s", output_path)


if __name__ == "__main__":
    main()
