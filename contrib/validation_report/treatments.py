"""Name and address mangling and treatment functions for fixture generation.

Each treatment function takes a string and an rng: random.Random, and returns
a (possibly modified) string. The rng parameter is used for deterministic
operation when seeded from the entity ID.
"""

import random
import re
from collections import defaultdict
from collections.abc import Callable
from typing import Any

from rigour.addresses import address_fingerprint
from rigour.territories import lookup_territory
from rigour.territories.territory import Territory


def switch_random_character(s: str, rng: random.Random) -> str:
    """Switch two adjacent characters in a string. e.g. Joe Biden -> Jeo Biden"""
    if len(s) <= 1:
        return s
    i = rng.randint(1, len(s) - 1)
    return s[: i - 1] + s[i] + s[i - 1] + s[i + 1 :]


def second_name_last_name_first_names(s: str, rng: random.Random) -> str:
    """Switch the first and last names. e.g. Joe Biden -> Biden, Joe"""
    if len(s) == 0:
        return s
    names = s.split(" ")
    if len(names) < 2:
        return s
    return " ".join(names[1:]) + ", " + names[0]


def replace_spaces_with_special_char(s: str, rng: random.Random) -> str:
    """Replace all spaces with a non-breaking space."""
    return s.replace(" ", "\u00a0")


def replace_non_ascii_with_special_char(s: str, rng: random.Random) -> str:
    """Replace all non-ASCII characters with '?'. e.g. Schrödinger -> Schr?dinger"""
    return "".join([c if ord(c) < 128 else "?" for c in s])


def replace_double_character_with_single(s: str, rng: random.Random) -> str:
    """Replace double characters with single. e.g. Picasso -> Picaso"""
    return "".join([c for i, c in enumerate(s) if i == 0 or s[i - 1] != c])


def remove_special_characters(s: str, rng: random.Random) -> str:
    """Remove all non-ASCII characters. e.g. Schrödinger -> Schrdinger"""
    return "".join([c if ord(c) < 128 else "" for c in s])


def duplicate_random_character(s: str, rng: random.Random) -> str:
    """Duplicate a random character. e.g. Pablo -> Pabblo"""
    if len(s) == 0:
        return s
    i = rng.randint(0, len(s) - 1)
    return s[:i] + s[i] + s[i:]


def replace_random_vowel(s: str, rng: random.Random) -> str:
    """Replace a random vowel with another vowel. e.g. Pablo -> Pabla"""
    vowels = "aeiouy"
    if len(s) == 0:
        return s
    i = rng.randint(0, len(s) - 1)
    if s[i] in vowels:
        return s[:i] + rng.choice(vowels) + s[i + 1 :]
    return s


def noop(s: str, rng: random.Random) -> str:
    return s


Treatment = Callable[[str, random.Random], str]

TREATMENTS: dict[str, Treatment] = {
    "switch_random_character": switch_random_character,
    "second_name_last_name_first_names": second_name_last_name_first_names,
    "replace_spaces_with_special_char": replace_spaces_with_special_char,
    "replace_non_ascii_with_special_char": replace_non_ascii_with_special_char,
    "replace_double_character_with_single": replace_double_character_with_single,
    "remove_special_characters": remove_special_characters,
    "duplicate_random_character": duplicate_random_character,
    "replace_random_vowel": replace_random_vowel,
}


def apply_treatment(entity: dict[str, Any], rng: random.Random) -> dict[str, Any]:
    """Apply deterministic name mangling or birthdate tweak to a slim FTM entity.

    30% chance: no change. 60% chance: mangle the name. 10% chance: tweak birthDate.
    """
    rand = rng.random()
    if rand < 0.3:
        return entity
    elif rand < 0.9:
        name_val = entity["properties"].get("name", [""])[0]
        if name_val:
            treatment_name = rng.choice(list(TREATMENTS.keys()))
            entity["properties"]["name"] = [TREATMENTS[treatment_name](name_val, rng)]
        return entity
    else:
        bd = entity["properties"].get("birthDate", [""])[0]
        if bd:
            parts = bd.split("-")
            idx = rng.choice(list(range(len(parts))))
            parts[idx] = str(int(parts[idx]) + 1)
            entity["properties"]["birthDate"] = ["-".join(parts)]
        return entity


ADDRESS_KEYWORD_SWAPS = [
    ("street", "st."),
    ("road", "rd."),
    ("avenue", "ave."),
    ("boulevard", "blvd."),
    ("building", "bldg."),
    ("улица", "ул."),
    ("проспект", "пр-кт"),
    ("дом", "д."),
    ("строение", "стр."),
    ("вулиця", "вул."),
    ("будинок", "буд."),
]

# BGN/PCGN-style romanization of Russian and Ukrainian, deliberately not the
# ICU scheme rigour uses, the way a Western party might transcribe an address.
BGN_ROMANIZATION = dict(
    zip(
        "абвгдезийклмнопрстуфыэі",
        "a b v g d e z i y k l m n o p r s t u f y e i".split(),
    )
) | {
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


def address_typo(s: str, rng: random.Random) -> str:
    """Replace a letter inside a word of six or more letters. e.g. Moscow -> Mosxow"""
    words = list(re.finditer(r"[^\W\d_]{6,}", s))
    if not words:
        return s
    word = rng.choice(words)
    i = rng.randrange(word.start() + 1, word.end() - 1)
    return s[:i] + ("x" if s[i].lower() != "x" else "z") + s[i + 1 :]


def address_swap_keyword(s: str, rng: random.Random) -> str:
    """Abbreviate or expand one address keyword. e.g. Main Street -> Main st."""
    for long, short in ADDRESS_KEYWORD_SWAPS:
        for find, repl in ((long, short), (short, long)):
            pattern = re.compile(rf"(?<!\w){re.escape(find)}(?!\w)", re.I)
            if pattern.search(s):
                return pattern.sub(repl, s, count=1)
    return s


def address_romanize(s: str, rng: random.Random) -> str:
    """Romanize Cyrillic BGN-style. e.g. ул. Тверская -> ul. Tverskaya"""
    all_caps = s == s.upper()
    out = []
    for char in s:
        low = char.lower()
        rom = BGN_ROMANIZATION.get(low)
        if rom is None:
            out.append(char)
        elif char == low:
            out.append(rom)
        else:
            out.append(rom.upper() if all_caps else rom.capitalize())
    return "".join(out)


def address_drop_last_part(s: str, rng: random.Random) -> str:
    """Drop the last comma-separated part, often the country or a postcode."""
    parts = [p.strip() for p in s.split(",") if p.strip()]
    return ", ".join(parts[:-1]) if len(parts) >= 3 else s


ADDRESS_TREATMENTS: dict[str, Treatment] = {
    "address_typo": address_typo,
    "address_swap_keyword": address_swap_keyword,
    "address_romanize": address_romanize,
    "address_drop_last_part": address_drop_last_part,
}


def apply_address_treatment(
    entity: dict[str, Any], rng: random.Random
) -> dict[str, Any]:
    """Apply a deterministic address treatment to a slim FTM entity.

    20% chance: no change. Otherwise one address treatment, falling back to the
    next one until the address changes; Cyrillic addresses are romanized half of
    the time, since that is the variation address matching most needs to bridge.
    """
    addresses = entity["properties"].get("address", [])
    if not addresses or rng.random() < 0.2:
        return entity
    address = addresses[0]
    names = list(ADDRESS_TREATMENTS)
    start = rng.randrange(len(names))
    order = names[start:] + names[:start]
    if re.search(r"[\u0400-\u04ff]", address) and rng.random() < 0.5:
        order = ["address_romanize"] + order
    for name in order:
        treated = ADDRESS_TREATMENTS[name](address, rng)
        if treated != address:
            entity["properties"]["address"] = [treated]
            break
    return entity


# Near misses: one part of an address changed so that it names a different place.
# Each returns None where the address has no such part.

HOUSE_NUMBER = re.compile(r"(?<![\w/\-.])(\d{1,4})(?![\w/\-])")
STREET_KEYWORD = (
    r"street|str\.|st\.|road|rd\.|avenue|ave\.|boulevard|blvd\.|ул\.|улица|вул\.|"
    r"вулиця|пр-кт|проспект|пр\.|ш\.|шоссе|пер\.|переулок"
)
STREET_AFTER_KEYWORD = re.compile(rf"(?i)(?<!\w)({STREET_KEYWORD})\s*([^\W\d_]{{4,}})")
STREET_BEFORE_KEYWORD = re.compile(rf"(?i)([^\W\d_]{{4,}})\s+({STREET_KEYWORD})(?!\w)")


def change_number(number: str, rng: random.Random) -> str:
    return str(int(number) + rng.randint(1, 9))


def near_miss_house_number(s: str, rng: random.Random) -> str | None:
    """A different house number on the same street. e.g. Main St 12 -> Main St 15"""
    match = HOUSE_NUMBER.search(s)
    if match is None:
        return None
    return s[: match.start(1)] + change_number(match.group(1), rng) + s[match.end(1) :]


def street_names(addresses: list[str]) -> list[str]:
    """Street names next to a street keyword, to swap into near misses."""
    names: set[str] = set()
    for address in addresses:
        for match in STREET_AFTER_KEYWORD.finditer(address):
            names.add(match.group(2))
        for match in STREET_BEFORE_KEYWORD.finditer(address):
            names.add(match.group(1))
    return sorted(names)


def near_miss_street(s: str, rng: random.Random, names: list[str]) -> str | None:
    """Another street of the same script in the same place, house number kept.
    e.g. ул. Тверская, д. 7 -> ул. Арбат, д. 7"""
    match = STREET_AFTER_KEYWORD.search(s)
    group = 2
    if match is None:
        match = STREET_BEFORE_KEYWORD.search(s)
        group = 1
    if match is None:
        return None
    current = match.group(group)
    cyrillic = re.search(r"[Ѐ-ӿ]", current) is not None
    candidates = [
        n
        for n in names
        if n.lower() != current.lower()
        and (re.search(r"[Ѐ-ӿ]", n) is not None) == cyrillic
    ]
    if not candidates:
        return None
    other = rng.choice(candidates)
    other = other.upper() if current.isupper() else other
    return s[: match.start(group)] + other + s[match.end(group) :]


TerritoryParts = dict[str, list[str]]
# Address parts that can name a territory: words only, at least four letters, so
# that codes (`CA` may be a US state) and fragments like `(CAR` are left out.
TERRITORY_PART = re.compile(
    r"(?=(?:[^\W\d_][^\w]*){4})[^\W\d_][^\W\d_ '.\-]*(?:[ '.\-]+[^\W\d_]+)*"
)


def is_cyrillic(s: str) -> bool:
    return re.search(r"[\u0400-\u04ff]", s) is not None


def named_territory(part: str) -> Territory | None:
    """The territory an address part names, if it is a territory name. Rigour's
    lookup also resolves cities to their country (Tehran -> Iran); the address
    fingerprint only codes names of the territory itself."""
    if TERRITORY_PART.fullmatch(part) is None:
        return None
    territory = lookup_territory(part)
    if territory is None or address_fingerprint(part) != territory.code:
        return None
    return territory


def territory_parts(addresses: list[str]) -> TerritoryParts:
    """Comma-separated address parts that name a country or subdivision, keyed by
    territory code (`ru`, `ru-ta`)."""
    parts: dict[str, set[str]] = defaultdict(set)
    for address in addresses:
        for part in address.split(","):
            part = part.strip().rstrip(".")
            territory = named_territory(part)
            if territory is not None:
                parts[territory.code].add(part)
    return {code: sorted(names) for code, names in parts.items()}


def swap_territory(
    s: str, rng: random.Random, parts: TerritoryParts, country: bool
) -> str | None:
    """Replace the first part naming a country (or a subdivision) with the name of
    another country (or another subdivision of the same country), in the same
    script."""
    pieces = s.split(",")
    for i, piece in enumerate(pieces):
        part = piece.strip().rstrip(".")
        territory = named_territory(part)
        if territory is None or territory.is_country != country:
            continue
        prefix = territory.code.split("-")[0]
        candidates = [
            name
            for code, names in parts.items()
            if code != territory.code
            and ("-" not in code) == country
            and (country or code.split("-")[0] == prefix)
            for name in names
            if is_cyrillic(name) == is_cyrillic(part)
        ]
        if not candidates:
            return None
        other = rng.choice(candidates)
        other = other.upper() if part.isupper() else other
        pieces[i] = piece.replace(part, other)
        return ",".join(pieces)
    return None


def near_miss_country(s: str, rng: random.Random, parts: TerritoryParts) -> str | None:
    """The same address in another country. e.g. ..., Kharkiv, Ukraine -> ..., Kharkiv, Russia"""
    return swap_territory(s, rng, parts, country=True)


def near_miss_region(s: str, rng: random.Random, parts: TerritoryParts) -> str | None:
    """The same address in another region of the same country.
    e.g. ..., Тверская область -> ..., Республика Татарстан"""
    return swap_territory(s, rng, parts, country=False)
