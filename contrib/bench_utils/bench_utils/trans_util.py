"""Transliteration helpers for generating test and benchmark data."""

import re
from functools import cache

import icu  # type: ignore[import-untyped]

# Letters used in Ukrainian but not Russian. Text containing any of them is
# romanized with the Ukrainian table, everything else with the Russian one.
UKRAINIAN_LETTERS = re.compile(r"[іїєґІЇЄҐ]")

# BGN/PCGN marks hard and soft signs with primes and keeps the diaeresis on ë.
# A Western party transcribing an address would drop both.
SIMPLIFY = str.maketrans({"ʹ": None, "ʺ": None, "ë": "e", "Ë": "E"})


@cache
def _transliterator(transform_id: str) -> icu.Transliterator:
    return icu.Transliterator.createInstance(transform_id)


def romanize_bgn(text: str) -> str:
    """Romanize Russian or Ukrainian Cyrillic using ICU's BGN/PCGN transforms,
    deliberately not the ICU scheme rigour uses, the way a Western party might
    transcribe an address. e.g. ул. Тверская -> ul. Tverskaya"""
    if UKRAINIAN_LETTERS.search(text):
        transform_id = "Ukrainian-Latin/BGN"
    else:
        transform_id = "Russian-Latin/BGN"
    romanized = _transliterator(transform_id).transliterate(text)
    return romanized.translate(SIMPLIFY)
