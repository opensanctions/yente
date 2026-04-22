"""Name mangling and treatment functions for fixture generation.

Each treatment function takes a string and an rng: random.Random, and returns
a (possibly modified) string. The rng parameter is used for deterministic
operation when seeded from the entity ID.
"""

import random
from typing import Any, Callable


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
