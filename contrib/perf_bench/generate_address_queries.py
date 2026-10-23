"""Generate an address-heavy query set for perf_bench from an FtM export.

Queries are real addresses of LegalEntity records, perturbed the way query data
differs from indexed data, so the fuzzy address clauses have work to do:

    python contrib/perf_bench/generate_address_queries.py \\
        /path/to/default/entities.ftm.json contrib/perf_bench/queries_address.json

Groups (tags):
  name-address   name plus a street-level address (5+ fingerprint tokens)
  address-only   a street-level address without a name
  vague          name plus a city-level address (3 or fewer tokens); these match
                 the most documents and are the worst case for the token clauses

Every query also carries its perturbation as a tag (`p-exact`, `p-typo`,
`p-abbrev`, `p-translit`, `p-drop-last`), `cyrillic` where the address is, and its
schema in lower case.
"""

import json
import random
import re
import sys
from collections.abc import Callable
from typing import Any

import orjson
from followthemoney import model
from rigour.addresses import address_fingerprint
from rigour.text.scripts import is_dense_script, text_scripts

SEED = 23
NAME_ADDRESS = 120
ADDRESS_ONLY = 60
VAGUE = 40
CYRILLIC_SHARE = 0.25

KEYWORD_SWAPS = [
    ("street", "st."),
    ("road", "rd."),
    ("avenue", "ave."),
    ("boulevard", "blvd."),
    ("building", "bldg."),
    ("улица", "ул."),
    ("проспект", "пр-кт"),
    ("дом", "д."),
    ("строение", "стр."),
]

# BGN/PCGN-style romanization, deliberately not rigour's ICU scheme.
BGN = dict(
    zip(
        "абвгдезийклмнопрстуфыэі",
        "a b v g d e z i y k l m n o p r s t u f y e i".split(),
    )
)
BGN.update(
    {
        "ё": "yo",
        "ж": "zh",
        "х": "kh",
        "ц": "ts",
        "ч": "ch",
        "ш": "sh",
        "щ": "shch",
        "ъ": "",
        "ь": "",
        "ю": "yu",
        "я": "ya",
        "ї": "yi",
        "є": "ye",
        "ґ": "g",
    }
)

Record = tuple[str, str, str]  # schema, name, address


def typo(addr: str, rng: random.Random) -> str | None:
    words = list(re.finditer(r"[^\W\d_]{6,}", addr))
    if not words:
        return None
    word = rng.choice(words)
    pos = rng.randrange(word.start() + 1, word.end() - 1)
    return addr[:pos] + ("x" if addr[pos].lower() != "x" else "z") + addr[pos + 1 :]


def abbrev(addr: str, rng: random.Random) -> str | None:
    for long, short in KEYWORD_SWAPS:
        for find, repl in ((long, short), (short, long)):
            pattern = re.compile(rf"(?<!\w){re.escape(find)}(?!\w)", re.I)
            if pattern.search(addr):
                return pattern.sub(repl, addr, count=1)
    return None


def translit(addr: str, rng: random.Random) -> str | None:
    if "Cyrillic" not in text_scripts(addr):
        return None
    all_caps = addr == addr.upper()
    out = []
    for char in addr:
        low = char.lower()
        rom = BGN.get(low)
        if rom is None:
            out.append(char)
        elif char == low:
            out.append(rom)
        else:
            out.append(rom.upper() if all_caps else rom.capitalize())
    return "".join(out)


def drop_last(addr: str, rng: random.Random) -> str | None:
    parts = [p.strip() for p in addr.split(",") if p.strip()]
    return ", ".join(parts[:-1]) if len(parts) >= 3 else None


PERTURBATIONS: list[tuple[str, Callable[[str, random.Random], str | None]]] = [
    ("exact", lambda a, r: a),
    ("typo", typo),
    ("abbrev", abbrev),
    ("translit", translit),
    ("drop-last", drop_last),
]


def load(path: str) -> tuple[list[Record], list[Record], list[Record]]:
    """Street-level Latin, street-level Cyrillic, and city-level records."""
    latin: list[Record] = []
    cyrillic: list[Record] = []
    vague: list[Record] = []
    with open(path, "rb") as fh:
        for line in fh:
            if b'"address"' not in line:
                continue
            ent = orjson.loads(line)
            schema = model.get(ent["schema"])
            if schema is None or not schema.is_a("LegalEntity"):
                continue
            props = ent.get("properties", {})
            names = props.get("name", [])
            if not names:
                continue
            for addr in props.get("address", []):
                if is_dense_script(addr):
                    continue
                fp = address_fingerprint(addr)
                if fp is None:
                    continue
                tokens = len(fp.split())
                record = (schema.name, names[0], addr)
                if tokens <= 3:
                    vague.append(record)
                elif tokens >= 5:
                    if "Cyrillic" in text_scripts(addr):
                        cyrillic.append(record)
                    else:
                        latin.append(record)
    return latin, cyrillic, vague


def make_query(
    group: str, record: Record, idx: int, rng: random.Random
) -> dict[str, Any]:
    schema, name, addr = record
    # Rotate perturbations, falling back to the next applicable one. Romanization
    # is where fuzzy matching works hardest, so half of the Cyrillic addresses
    # try it first.
    start = idx % len(PERTURBATIONS)
    order = PERTURBATIONS[start:] + PERTURBATIONS[:start]
    if idx % 2 == 0 and "Cyrillic" in text_scripts(addr):
        order = [p for p in PERTURBATIONS if p[0] == "translit"] + order
    for label, perturb in order:
        query_addr = perturb(addr, rng)
        if query_addr is not None:
            break
    assert query_addr is not None
    properties: dict[str, list[str]] = {"address": [query_addr]}
    query_schema = "LegalEntity"
    if group != "address-only":
        properties["name"] = [name]
        query_schema = schema
    tags = ["addresses", group, f"p-{label}", query_schema.lower()]
    if "Cyrillic" in text_scripts(addr):
        tags.append("cyrillic")
    return {
        "id": f"{group}-{idx:03d}",
        "tags": tags,
        "query": {"schema": query_schema, "properties": properties},
    }


def main(source: str, target: str) -> None:
    rng = random.Random(SEED)
    latin, cyrillic, vague = load(source)

    def street_sample(n: int) -> list[Record]:
        n_cyr = min(len(cyrillic), round(n * CYRILLIC_SHARE))
        return rng.sample(cyrillic, n_cyr) + rng.sample(latin, n - n_cyr)

    groups = {
        "name-address": street_sample(NAME_ADDRESS),
        "address-only": street_sample(ADDRESS_ONLY),
        "vague": rng.sample(vague, VAGUE),
    }
    queries = [
        make_query(group, record, idx, rng)
        for group, records in groups.items()
        for idx, record in enumerate(records)
    ]
    with open(target, "w", encoding="utf-8") as fh:
        json.dump(queries, fh, indent=2, ensure_ascii=False)
        fh.write("\n")
    print(
        f"{len(latin):,} Latin and {len(cyrillic):,} Cyrillic street-level, "
        f"{len(vague):,} city-level addresses; wrote {len(queries)} queries"
    )


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
