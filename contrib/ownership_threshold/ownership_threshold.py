"""Apply a percentage threshold to the ownership data in a yente index.

The `sanction.control` topic in OpenSanctions marks entities owned by a sanctioned
party, directly or through subsidiaries, without a percentage threshold. This script
reads the same FollowTheMoney export that yente indexes (for example
`default/entities.ftm.json`) and runs the OFAC 50 Percent Rule and the BIS
Affiliates Rule over its Ownership links with the `ownership-rules` package. Each
flagged entity gets a verdict and the chain of owners and shares behind it.

    pip install ownership-rules  # or the release wheel, see README.md
    python ownership_threshold.py entities.ftm.json --out verdicts.jsonl

Results are evidence for a compliance review, not a legal determination.
"""

import argparse
import json
import re
import sys
import time
from collections import Counter
from collections.abc import Iterator
from typing import Any, TextIO

from orjson import loads
from ownership_rules import evaluate
from ownership_rules.formats import from_ftm

LINK_SCHEMAS = {"Ownership", "Sanction"}
# Properties of linked entities that the rules use: list membership comes from
# `datasets`, countries limit the BIS rule to foreign companies.
KEEP_PROPS = ("name", "country", "jurisdiction", "mainCountry", "topics")
REF_PROPS = ("owner", "asset", "entity", "director", "organization")
LINK_PROPS = REF_PROPS + (
    "percentage",
    "ownershipType",
    "role",
    "startDate",
    "endDate",
    "program",
    "programId",
)
SCHEMA_RE = re.compile(r'"schema"\s*:\s*"(Ownership|Sanction|Directorship)"')
CONTROL_TOPIC = "sanction.control"


def read_entities(path: str) -> Iterator[dict[str, Any]]:
    with open(path, "rb") as fh:
        for line in fh:
            if line.strip():
                yield loads(line)


def slim(entity: dict[str, Any], keep: tuple[str, ...]) -> dict[str, Any]:
    props = entity.get("properties", {})
    return {
        "id": entity.get("id"),
        "schema": entity.get("schema"),
        "caption": entity.get("caption"),
        "datasets": entity.get("datasets", []),
        "properties": {k: props[k] for k in keep if k in props},
    }


def load(
    paths: list[str], directorships: bool = False
) -> tuple[list[dict[str, Any]], set[str]]:
    """Read the links in a first pass, then only the entities they reference, so a
    full `default` export does not have to be held in memory."""
    link_schemas = LINK_SCHEMAS | ({"Directorship"} if directorships else set())
    links: list[dict[str, Any]] = []
    wanted: set[str] = set()
    for path in paths:
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                if not SCHEMA_RE.search(line):
                    continue
                entity = loads(line)
                if entity.get("schema") not in link_schemas:
                    continue
                props = entity.get("properties", {})
                links.append(slim(entity, LINK_PROPS))
                for prop in REF_PROPS:
                    wanted.update(v for v in props.get(prop, []) if isinstance(v, str))

    nodes: list[dict[str, Any]] = []
    controlled: set[str] = set()
    for path in paths:
        for entity in read_entities(path):
            if entity.get("schema") in link_schemas:
                continue
            props = entity.get("properties", {})
            if CONTROL_TOPIC in props.get("topics", []):
                controlled.add(entity["id"])
            if entity.get("id") not in wanted:
                continue
            nodes.append(slim(entity, KEEP_PROPS))
    return nodes + links, controlled


def caption(entity: dict[str, Any] | None) -> str | None:
    if entity is None:
        return None
    names = entity.get("properties", {}).get("name") or [None]
    value = entity.get("caption") or names[0]
    return str(value) if value is not None else None


def run(
    paths: list[str],
    rules: list[str],
    out: TextIO,
    as_of: str | None = None,
    directorships: bool = False,
    log: TextIO = sys.stderr,
) -> dict[str, Counter[str]]:
    started = time.monotonic()
    entities, controlled = load(paths, directorships=directorships)
    graph = from_ftm(entities, directorships=directorships)
    by_id = {e["id"]: e for e in entities if e.get("schema") not in LINK_SCHEMAS}
    print(
        f"read {len(entities)} entities: {len(graph.edges)} ownership edges, "
        f"{len(graph.listed)} sanctioned parties, {len(controlled)} tagged "
        f"{CONTROL_TOPIC} ({time.monotonic() - started:.1f}s)",
        file=log,
    )

    summary: dict[str, Counter[str]] = {}
    for rule in rules:
        started = time.monotonic()
        ev = evaluate(
            graph.edges, graph.listed, rule, entities=graph.entities, as_of=as_of
        )
        counts: Counter[str] = Counter()
        for result in ev.flagged():
            counts[result.verdict] += 1
            row = result.to_json(proofs="none")
            row.update(
                rule=ev.rule.id,
                caption=caption(by_id.get(result.entity)),
                topics=by_id.get(result.entity, {})
                .get("properties", {})
                .get("topics", []),
                explain=result.explain(),
            )
            out.write(json.dumps(row, ensure_ascii=False) + "\n")
        tagged: Counter[str] = Counter(ev.verdict(eid) for eid in controlled)
        summary[ev.rule.id] = counts
        print(
            f"{ev.rule.id}: {dict(counts)} ({time.monotonic() - started:.1f}s); "
            f"{CONTROL_TOPIC} entities by verdict: {dict(tagged)}",
            file=log,
        )
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("paths", nargs="+", help="FtM entity files (entities.ftm.json)")
    parser.add_argument(
        "--rule",
        action="append",
        help="ownership-rules rule id or rule file (default: ofac-50 and bis-affiliates)",
    )
    parser.add_argument(
        "--out", help="write verdicts as JSON lines here (default: stdout)"
    )
    parser.add_argument(
        "--as-of", help="evaluate on this day (YYYY-MM-DD, default today)"
    )
    parser.add_argument(
        "--directorships",
        action="store_true",
        help="count a sanctioned director as control (reported as control_only)",
    )
    args = parser.parse_args(argv)
    rules = args.rule or ["ofac-50", "bis-affiliates"]
    if args.out:
        with open(args.out, "w", encoding="utf-8") as out:
            run(args.paths, rules, out, args.as_of, args.directorships)
    else:
        run(args.paths, rules, sys.stdout, args.as_of, args.directorships)
    return 0


if __name__ == "__main__":
    sys.exit(main())
