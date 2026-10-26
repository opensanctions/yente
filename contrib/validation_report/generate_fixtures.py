#!/usr/bin/env python3
"""Download OpenSanctions FTM data and generate deterministic fixture JSON files."""

import copy
import csv
import hashlib
import logging
import random
from collections.abc import Callable
from pathlib import Path
from typing import Any

import click
import httpx
import orjson
from followthemoney import model
from rigour.addresses import address_fingerprint
from rigour.text.scripts import is_dense_script
from treatments import (
    apply_address_treatment,
    apply_treatment,
    near_miss_country,
    near_miss_house_number,
    near_miss_region,
    near_miss_street,
    street_names,
    territory_parts,
)

log = logging.getLogger(__name__)

BUILD_DIR = Path(__file__).parent / "build" / "fixtures"
STATIC_FIXTURES_DIR = Path(__file__).parent / "fixtures"

# The runs of each fixture in generate_report_data.py. A weight for a feature the
# algorithm lacks is ignored, so against yente versions without `address_match`
# the weighted run equals plain logic-v2. Weights at or below the 0.7 threshold
# never return an address-only match: /match drops results scoring at or below
# the cutoff, which defaults to the threshold.
ADDRESS_MATCH_WEIGHT = 0.9
LOGIC_V1: dict[str, Any] = {"algorithm": "logic-v1"}
LOGIC_V2: dict[str, Any] = {"algorithm": "logic-v2"}
LOGIC_V2_ADDRESS: dict[str, Any] = {
    "algorithm": "logic-v2",
    "weights": {"address_match": ADDRESS_MATCH_WEIGHT},
}
# Fixtures without addresses run without address_match, which can't affect them.
NAME_RUNS = [LOGIC_V1, LOGIC_V2]
ADDRESS_RUNS = [LOGIC_V1, LOGIC_V2, LOGIC_V2_ADDRESS]

POSITIVE_FIXTURES: list[dict[str, Any]] = [
    {
        "output_filename": "positives_un_treated.json",
        "dataset": "un_sc_sanctions",
        "treatment": True,
        "description": (
            "Generated from the `un_sc_sanctions` dataset, treated version: "
            "minor typos and name reshuffles applied. These are true-positives. "
            "Higher ID Matched is better."
        ),
    },
    {
        "output_filename": "positives_un_untreated.json",
        "dataset": "un_sc_sanctions",
        "treatment": False,
        "description": (
            "Generated from the `un_sc_sanctions` dataset. These are true-positives. "
            "Higher ID Matched is better."
        ),
    },
    {
        "output_filename": "positives_us_congress_untreated.json",
        "dataset": "us_congress",
        "treatment": False,
        "description": (
            "Generated from the `us_congress` dataset. These are true-positives. "
            "Higher ID Matched is better."
        ),
    },
]

# Datasets with many street-level addresses on legal entities: OFAC mixes
# scripts, NSDC is mostly Cyrillic. Each contributes ADDRESS_SAMPLE_PER_DATASET
# entities to the address positives.
ADDRESS_DATASETS = ["us_ofac_sdn", "ua_nsdc_sanctions"]
ADDRESS_SAMPLE_PER_DATASET = 125
ADDRESS_NEGATIVES = 250
NearMiss = Callable[[str, random.Random], str | None]
# Fewer fingerprint tokens than this is a city-level address or vaguer.
ADDRESS_MIN_TOKENS = 5

NEGATIVE_FIXTURES: list[dict[str, Any]] = [
    {
        "output_filename": "negatives_global.json",
        "source_csv": "negatives_global.csv",
        "description": (
            "Global true-negatives: a reference dataset of synthetic person records "
            "with multi-cultural name diversity (Western European, Slavic, African, "
            "East Asian, Southeast Asian, and others) and geographic correlation. "
            "Any match is a false-positive."
        ),
    },
    {
        "output_filename": "negatives_us.json",
        "source_csv": "negatives_us.csv",
        "description": (
            "US true-negatives: a reference dataset of synthetic US-based person records "
            "to test false-positive rates for US-centric screening. Any match is a "
            "false-positive."
        ),
    },
]


def file_md5(path: Path) -> str:
    h = hashlib.md5()
    h.update(path.read_bytes())
    return h.hexdigest()


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


def street_address(addresses: list[str]) -> str | None:
    """The first address that names at least a street, in a script rigour
    tokenises (not Chinese, Japanese or Korean)."""
    for address in addresses:
        if is_dense_script(address):
            continue
        fingerprint = address_fingerprint(address)
        if fingerprint is not None and len(fingerprint.split()) >= ADDRESS_MIN_TOKENS:
            return address
    return None


def parse_address_entity(line: bytes) -> dict[str, Any] | None:
    """Slim a LegalEntity record with a name and a street-level address down to
    those two properties."""
    entity = orjson.loads(line)
    schema = model.get(entity.get("schema", ""))
    if schema is None or not schema.is_a("LegalEntity"):
        return None
    props = entity.get("properties", {})
    names = props.get("name", [])
    address = street_address(props.get("address", []))
    if not names or address is None:
        return None
    return {
        "id": entity["id"],
        "schema": schema.name,
        "properties": {"name": [names[0]], "address": [address]},
    }


def entity_rng(entity_id: str) -> random.Random:
    seed = int(hashlib.sha256(entity_id.encode()).hexdigest(), 16) % (2**32)
    return random.Random(seed)


def write_fixture(
    filename: str,
    type_: str,
    description: str,
    generated_from: str,
    data: list[dict[str, Any]],
    runs: list[dict[str, Any]],
) -> None:
    output = {
        "name": filename,
        "type": type_,
        "description": description,
        "generated_from": generated_from,
        "runs": runs,
        "data": data,
    }
    output_path = BUILD_DIR / filename
    output_path.write_bytes(orjson.dumps(output, option=orjson.OPT_INDENT_2))
    log.info("Wrote %s (%d records)", output_path, len(data))


def csv_row_to_ftm_entity(row: dict[str, str]) -> dict[str, Any]:
    """Convert a screening-fixtures CSV row to a slim FTM entity."""
    props: dict[str, list[str]] = {}
    if row.get("full_name"):
        props["name"] = [row["full_name"]]
    if row.get("first_name"):
        props["firstName"] = [row["first_name"]]
    if row.get("middle_name"):
        props["middleName"] = [row["middle_name"]]
    if row.get("last_name"):
        props["lastName"] = [row["last_name"]]
    if row.get("gender"):
        props["gender"] = [row["gender"]]
    if row.get("date_of_birth"):
        props["birthDate"] = [row["date_of_birth"]]
    if row.get("place_of_birth"):
        props["birthPlace"] = [row["place_of_birth"]]
    if row.get("nationality"):
        props["nationality"] = [row["nationality"]]
    # Generate a deterministic synthetic ID from the row content
    row_bytes = orjson.dumps(row, option=orjson.OPT_SORT_KEYS)
    synthetic_id = "synthetic-" + hashlib.sha256(row_bytes).hexdigest()[:16]
    return {"id": synthetic_id, "schema": "Person", "properties": props}


def fetch_dataset_version(client: httpx.Client, dataset: str) -> str:
    url = f"https://data.opensanctions.org/datasets/latest/{dataset}/index.json"
    resp = client.get(url, follow_redirects=True)
    resp.raise_for_status()
    return str(resp.json()["version"])


def download_ftm_entities(client: httpx.Client, dataset: str) -> bytes:
    url = f"https://data.opensanctions.org/datasets/latest/{dataset}/entities.ftm.json"
    log.info("Downloading %s ...", url)
    resp = client.get(url, follow_redirects=True)
    resp.raise_for_status()
    return resp.content


def generate_address_fixtures(client: httpx.Client) -> None:
    """Positives with addresses from ADDRESS_DATASETS, and negatives pairing
    synthetic names with listed addresses or near misses of them."""
    entities: list[dict[str, Any]] = []
    pool: list[str] = []
    sources: list[str] = []
    for dataset in ADDRESS_DATASETS:
        version = fetch_dataset_version(client, dataset)
        sources.append(f"{dataset} {version}")
        parsed_entities: list[dict[str, Any]] = []
        for line in download_ftm_entities(client, dataset).splitlines():
            line = line.strip()
            if line:
                parsed = parse_address_entity(line)
                if parsed is not None:
                    parsed_entities.append(parsed)
        log.info(
            "Parsed %d entities with addresses from %s", len(parsed_entities), dataset
        )
        parsed_entities.sort(key=lambda e: e["id"])
        pool.extend(e["properties"]["address"][0] for e in parsed_entities)
        entities.extend(
            random.Random(dataset).sample(
                parsed_entities,
                min(ADDRESS_SAMPLE_PER_DATASET, len(parsed_entities)),
            )
        )
    generated_from = ", ".join(sources)

    treated = copy.deepcopy(entities)
    for entity in treated:
        rng = entity_rng(entity["id"])
        apply_treatment(entity, rng)
        apply_address_treatment(entity, rng)
    write_fixture(
        "positives_addresses_treated.json",
        "positive",
        "Legal entities from `us_ofac_sdn` and `ua_nsdc_sanctions`, name and "
        "address treated: typos and reshuffles in the name; a typo, keyword "
        "abbreviation, BGN-style romanization or dropped trailing part in the "
        "address. These are true-positives. Higher ID Matched is better; the "
        "address_match run should be no worse than logic-v2.",
        generated_from,
        treated,
        ADDRESS_RUNS,
    )
    address_only = [
        {**e, "properties": {"address": e["properties"]["address"]}} for e in treated
    ]
    write_fixture(
        "positives_address_only.json",
        "positive",
        "The same legal entities as positives_addresses_treated, queried by "
        "their treated address alone. These are true-positives. Higher ID "
        "Matched is better.",
        generated_from,
        address_only,
        [LOGIC_V2_ADDRESS],
    )

    # Each fixture below takes its own slice of synthetic names.
    names = iter(read_negative_names("negatives_global.csv"))
    listed = sorted(set(pool))
    listed_set = set(listed)

    rng = random.Random("listed_addresses")
    rows = [
        synthetic_legal_entity(next(names), address)
        for address in rng.sample(listed, ADDRESS_NEGATIVES)
    ]
    write_fixture(
        "negatives_listed_addresses.json",
        "negative",
        "Synthetic names, each with the street-level address of a listed legal "
        "entity. Any match is a false-positive: a weak match might be lifted by the "
        "address.",
        generated_from,
        rows,
        [LOGIC_V1, LOGIC_V2],
    )

    # Near misses: a listed address changed to name a different place, the kinds
    # of change in equal parts. Changes that happen to produce a listed address
    # are skipped.
    streets = street_names(listed)
    territories = territory_parts(listed)
    near_misses: list[NearMiss] = [
        near_miss_house_number,
        lambda a, r: near_miss_street(a, r, streets),
        lambda a, r: near_miss_country(a, r, territories),
        lambda a, r: near_miss_region(a, r, territories),
    ]
    rng = random.Random("near_miss_addresses")
    rows = []
    for i, near_miss in enumerate(near_misses):
        target = (i + 1) * ADDRESS_NEGATIVES // len(near_misses)
        for original in rng.sample(listed, len(listed)):
            if len(rows) >= target:
                break
            address = near_miss(original, rng)
            if address is not None and address not in listed_set:
                rows.append(synthetic_legal_entity(next(names), address))
    write_fixture(
        "negatives_near_miss_addresses.json",
        "negative",
        "Synthetic names, each with an address similar to a listed one but naming a "
        "strictly different place: a listed street-level address with another "
        "house number, street, country or region, in equal parts. Any match is a "
        "false-positive; compare the address_match run with logic-v2 for the "
        "false-positives addresses cause.",
        generated_from,
        rows,
        [LOGIC_V2, LOGIC_V2_ADDRESS],
    )


def read_negative_names(csv_name: str) -> list[dict[str, str]]:
    with open(STATIC_FIXTURES_DIR / csv_name, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def synthetic_legal_entity(row: dict[str, str], address: str) -> dict[str, Any]:
    """A synthetic name from a screening-fixtures CSV row with the given address,
    as a LegalEntity so that it can match companies at that address too."""
    row_bytes = orjson.dumps({**row, "address": address}, option=orjson.OPT_SORT_KEYS)
    return {
        "id": "synthetic-" + hashlib.sha256(row_bytes).hexdigest()[:16],
        "schema": "LegalEntity",
        "properties": {"name": [row["full_name"]], "address": [address]},
    }


@click.command()
def main() -> None:
    """Download OpenSanctions FTM data and generate fixture JSON files in build/fixtures/."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    BUILD_DIR.mkdir(parents=True, exist_ok=True)

    # Group positive fixtures by dataset to avoid duplicate downloads
    by_dataset: dict[str, list[dict[str, Any]]] = {}
    for fixture_config in POSITIVE_FIXTURES:
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
                    "type": "positive",
                    "description": fixture_config["description"],
                    "generated_from": f"{dataset} {version}",
                    "runs": NAME_RUNS,
                    "data": fixture_entities,
                }

                output_path = BUILD_DIR / fixture_config["output_filename"]
                output_path.write_bytes(
                    orjson.dumps(output, option=orjson.OPT_INDENT_2)
                )
                log.info("Wrote %s (%d records)", output_path, len(fixture_entities))

        generate_address_fixtures(client)

    # Generate negative fixtures from static CSV files
    for fixture_config in NEGATIVE_FIXTURES:
        csv_path = STATIC_FIXTURES_DIR / fixture_config["source_csv"]
        if not csv_path.exists():
            log.error("Source CSV not found: %s", csv_path)
            raise SystemExit(1)

        entities = []
        with open(csv_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                entities.append(csv_row_to_ftm_entity(row))

        source_md5 = file_md5(csv_path)
        output = {
            "name": fixture_config["output_filename"],
            "type": "negative",
            "description": fixture_config["description"],
            "generated_from": f"{fixture_config['source_csv']} md5:{source_md5}",
            "runs": NAME_RUNS,
            "data": entities,
        }

        output_path = BUILD_DIR / fixture_config["output_filename"]
        output_path.write_bytes(orjson.dumps(output, option=orjson.OPT_INDENT_2))
        log.info("Wrote %s (%d records)", output_path, len(entities))


if __name__ == "__main__":
    main()
