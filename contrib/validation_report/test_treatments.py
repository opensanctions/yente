"""Run with: pytest contrib/validation_report"""

import random

from treatments import (
    address_drop_last_part,
    address_romanize,
    address_swap_keyword,
    address_typo,
    apply_address_treatment,
    named_territory,
    near_miss_country,
    near_miss_house_number,
    near_miss_region,
    near_miss_street,
    street_names,
    territory_parts,
)


def rng() -> random.Random:
    return random.Random("test")


def test_address_typo() -> None:
    address = "Tverskaya 7, Moscow"
    typo = address_typo(address, rng())
    assert len(typo) == len(address)
    assert sum(a != b for a, b in zip(address, typo)) == 1
    assert address_typo("7, 12", rng()) == "7, 12"


def test_address_swap_keyword() -> None:
    assert address_swap_keyword("1 Main Street", rng()) == "1 Main st."
    # Russian abbreviation "ул." should be swapped to "улица" meaning "street"
    assert address_swap_keyword("ул. Тверская, д. 7", rng()) == "улица Тверская, д. 7"
    assert address_swap_keyword("Moscow", rng()) == "Moscow"


def test_address_romanize() -> None:
    assert address_romanize("ул. Тверская, д. 7", rng()) == "ul. Tverskaya, d. 7"
    assert address_romanize("Київ, вулиця Хрещатик", rng()) == (
        "Kyyiv, vulytsya Khreshchatyk"
    )


def test_address_drop_last_part() -> None:
    assert address_drop_last_part("1 Main St, Springfield, USA", rng()) == (
        "1 Main St, Springfield"
    )
    assert address_drop_last_part("1 Main St, Springfield", rng()) == (
        "1 Main St, Springfield"
    )


def test_near_miss_house_number() -> None:
    # ul. Tverskaya, d. 7, 125009 - building number 7 should be changed
    near_miss = near_miss_house_number("ул. Тверская, д. 7, 125009", rng())
    assert near_miss is not None
    number = int(near_miss.split("д. ")[1].split(",")[0])
    assert 8 <= number <= 16
    assert near_miss.endswith(", 125009")
    # Can't find a house number to modify
    assert near_miss_house_number("прим. 36/1Ч, корп. 2-Б", rng()) is None


def test_street_names() -> None:
    addresses = ["ул. Тверская, д. 7", "12 Main Street, Springfield"]
    assert street_names(addresses) == ["Main", "Тверская"]


def test_near_miss_street() -> None:
    # Arbat, Main, Tverskaya are the street names to consider for near misses.
    names = ["Арбат", "Main", "Тверская"]
    assert near_miss_street("ул. Тверская, д. 7", rng(), names) == "ул. Арбат, д. 7"
    assert near_miss_street("УЛ. ТВЕРСКАЯ, Д. 7", rng(), names) == "УЛ. АРБАТ, Д. 7"
    assert near_miss_street("12 Main Street", rng(), names) is None
    assert near_miss_street("Moscow, 125009", rng(), names) is None


def test_named_territory() -> None:
    russia = named_territory("Российская Федерация")
    assert russia is not None and russia.code == "ru"
    tatarstan = named_territory("Республика Татарстан")
    assert tatarstan is not None and tatarstan.code == "ru-ta"
    assert named_territory("CA") is None
    assert named_territory("Tehran") is None


ADDRESSES = [
    "ул. Тверская, д. 7, Москва, Российская Федерация",
    "вул. Хрещатик, 22, Київ, Україна",
    "Республика Татарстан, г. Казань, ул. Баумана, 3",
]


def test_territory_parts() -> None:
    parts = territory_parts(ADDRESSES)
    assert parts["ru"] == ["Российская Федерация"]
    assert parts["ua"] == ["Україна"]
    assert parts["ru-ta"] == ["Республика Татарстан"]


def test_near_miss_country() -> None:
    parts = territory_parts(ADDRESSES)
    assert near_miss_country(ADDRESSES[1], rng(), parts) == (
        "вул. Хрещатик, 22, Київ, Российская Федерация"
    )
    assert near_miss_country("12 Main Street, Springfield", rng(), parts) is None


def test_near_miss_region() -> None:
    parts = territory_parts(ADDRESSES)
    assert near_miss_region(ADDRESSES[2], rng(), parts) == (
        "Москва, г. Казань, ул. Баумана, 3"
    )
    # Kyiv is the only Ukrainian subdivision, so there is nothing to swap in.
    assert near_miss_region(ADDRESSES[1], rng(), parts) is None


# The cases below record where the near misses are too simplistic: the result is
# not a good negative, because it still names the same place, or because it
# contradicts itself and so is easy to reject without telling places apart.


def test_near_miss_house_number_simplistic() -> None:
    # The postcode changes, since `L - 2652` has spaces around the hyphen; the
    # house number 142-144 is kept, so this is the same building.
    near_miss = near_miss_house_number(
        "142-144, Rue Albert Unden L - 2652 Luxembourg", rng()
    )
    assert near_miss is not None
    assert near_miss.startswith("142-144, Rue Albert Unden L - 26")
    assert not near_miss.endswith("L - 2652 Luxembourg")
    # A distance changes, not the house number: the same building, described
    # as being somewhere else.
    # 0.6 km southwest of Mykhalevo village, Bldg. 1
    near_miss = near_miss_house_number(
        "вул. 0.6 км на Південний Захід с. Михалєво, буд. 1", rng()
    )
    assert near_miss is not None
    assert not near_miss.startswith("вул. 0.6 км")
    assert near_miss.endswith("км на Південний Захід с. Михалєво, буд. 1")


def test_near_miss_street_simplistic() -> None:
    # `St.` is read as a street keyword, so the city is swapped instead of street.
    assert near_miss_street(
        "prospekt Troitski, St. Petersburg, 190005", rng(), ["October"]
    ) == ("prospekt Troitski, St. October, 190005")
    # Only the first word of a two-word street name is swapped.
    assert near_miss_street(
        "вул. Академіка Петрова, буд. 2А", rng(), ["Талсінська"]
    ) == ("вул. Талсінська Петрова, буд. 2А")
