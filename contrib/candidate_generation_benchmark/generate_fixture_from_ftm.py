import orjson
import random
import csv
import sys
from typing import Dict, Optional
import click

# curl "https://data.opensanctions.org/datasets/20250820/un_sc_sanctions/entities.ftm.json" \
# | python contrib/candidate_generation_benchmark/generate_fixture_from_ftm.py \
# > contrib/candidate_generation_benchmark/fixtures/positives_un_treated.csv


def switch_random_character(s: str) -> str:
    """
    Switch two random characters in a string.
    Joe Biden -> Jeo Biden
    """
    if len(s) == 0:
        return s
    i = random.randint(1, len(s) - 1)
    return s[: i - 1] + s[i] + s[i - 1] + s[i + 1 :]


def second_name_last_name_first_names(s: str) -> str:
    """
    Switch the first and last names of a person, joining with a comma.
    Joe Biden -> Biden, Joe
    Pablo Ruiz Picasso -> Ruiz Picasso, Pablo
    """
    if len(s) == 0:
        return s
    names = s.split(" ")
    if len(names) < 2:
        return s
    return " ".join(names[1:]) + ", " + names[0]


def replace_spaces_with_special_char(s: str) -> str:
    """
    Replace all spaces with a non-breaking space.
    Pablo Picasso -> Pablo\u00a0Picasso
    """
    return s.replace(" ", " ")


def replace_non_ascii_with_special_char(s: str) -> str:
    """
    Replace all non-ascii characters with a special char.
    Schrödinger -> Schr?dinger
    """
    return "".join([c if ord(c) < 128 else "?" for c in s])


def replace_double_character_with_single(s: str) -> str:
    """
    Replace all double characters with a single character.
    Pablo Picasso -> Pablo Picaso
    """
    return "".join([c for i, c in enumerate(s) if i == 0 or s[i - 1] != c])


def remove_special_characters(s: str) -> str:
    """
    Remove all special characters.
    Schrödinger -> Schrdinger
    """
    return "".join([c if ord(c) < 128 else "" for c in s])


def duplicate_random_character(s: str) -> str:
    """
    Duplicate a random character in a string.
    Pablo Picasso -> Pabblo Picasso
    """
    if len(s) == 0:
        return s
    i = random.randint(0, len(s) - 1)
    return s[:i] + s[i] + s[i:]


def replace_random_vowel(s: str) -> str:
    """
    Replace a random vowel with another vowel.
    Pablo Picasso -> Pabla Picasso
    """
    vowels = "aeiouy"
    if len(s) == 0:
        return s
    i = random.randint(0, len(s) - 1)
    if s[i] in vowels:
        return s[:i] + random.choice(vowels) + s[i + 1 :]
    return s


def noop(s: str) -> str:
    return s


treatment_mapping = {
    "switch_random_character": switch_random_character,
    "second_name_last_name_first_names": second_name_last_name_first_names,
    "replace_spaces_with_special_char": replace_spaces_with_special_char,
    "replace_non_ascii_with_special_char": replace_non_ascii_with_special_char,
    "replace_double_character_with_single": replace_double_character_with_single,
    "remove_special_characters": remove_special_characters,
    "duplicate_random_character": duplicate_random_character,
    "replace_random_vowel": replace_random_vowel,
}


def parse_ftm_entity(line: str) -> Optional[Dict[str, str]]:
    """
    Parse a single FTM entity line and extract person information.
    Returns None if the entity is not a Person.
    """
    entity = orjson.loads(line.strip())

    # Skip if not a Person
    if entity.get("schema") != "Person":
        return None

    properties = entity.get("properties", {})

    # Extract name components
    full_name = properties.get("name", [""])[0] if properties.get("name") else ""
    # first_name = properties.get("firstName", [""])[0] if properties.get("firstName") else ""
    # middle_name = properties.get("secondName", [""])[0] if properties.get("secondName") else ""
    # last_name = properties.get("lastName", [""])[0] if properties.get("lastName") else ""
    first_name = ""
    middle_name = ""
    last_name = ""

    # If no full name but we have first and last, construct it
    if not full_name and (first_name or last_name):
        name_parts = [first_name, middle_name, last_name]
        full_name = " ".join([part for part in name_parts if part])

    # Extract other fields
    gender = properties.get("gender", [""])[0] if properties.get("gender") else ""
    date_of_birth = (
        properties.get("birthDate", [""])[0] if properties.get("birthDate") else ""
    )
    place_of_birth = (
        properties.get("birthPlace", [""])[0] if properties.get("birthPlace") else ""
    )
    nationality = (
        properties.get("nationality", [""])[0] if properties.get("nationality") else ""
    )

    return {
        "full_name": full_name,
        "first_name": first_name,
        "middle_name": middle_name,
        "last_name": last_name,
        "gender": gender,
        "date_of_birth": date_of_birth,
        "place_of_birth": place_of_birth,
        "nationality": nationality,
    }


def apply_treatment(person_data: Dict[str, str]) -> Dict[str, str]:
    """
    Apply treatment to a person record.
    In 30% of cases, do nothing. In the rest, randomly choose a name field
    and apply a random treatment to it.
    """
    rand = random.random()
    # 30% chance to do nothing
    if rand < 0.3:
        return person_data
    elif rand < 0.9:
        # Choose which name field to modify
        # name_fields = ["full_name", "first_name", "middle_name", "last_name"]
        name_fields = ["full_name"]
        field_to_modify = random.choice(name_fields)

        # Choose a random treatment (excluding noop)
        available_treatments = list(treatment_mapping.keys())
        treatment_name = random.choice(available_treatments)
        treatment_func = treatment_mapping[treatment_name]

        # Apply the treatment
        original_value = person_data[field_to_modify]
        if original_value:  # Only apply treatment if the field has a value
            person_data[field_to_modify] = treatment_func(original_value)

        return person_data
    else:
        # change the birth year by one year
        if person_data["date_of_birth"]:
            split_date = person_data["date_of_birth"].split("-")
            change_part = random.choice(range(len(split_date)))
            split_date[change_part] = str(int(split_date[change_part]) + 1)
            person_data["date_of_birth"] = "-".join(split_date)
        return person_data


@click.command()
def main() -> None:
    """
    Process FTM entities from stdin and apply treatments to person names.

    Reads JSON lines from stdin, filters for Person entities,
    applies random treatments to names, and outputs as CSV to stdout.
    """
    processed_records = []

    for line_num, line in enumerate(sys.stdin, 1):
        person_data = parse_ftm_entity(line)
        if person_data:
            # Apply treatment
            treated_data = apply_treatment(person_data)
            processed_records.append(treated_data)

    # Write to CSV on stdout
    if processed_records:
        fieldnames = [
            "full_name",
            "first_name",
            "middle_name",
            "last_name",
            "gender",
            "date_of_birth",
            "place_of_birth",
            "nationality",
        ]

        writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(processed_records)
    else:
        click.echo("No Person entities found in the input.", err=True)


if __name__ == "__main__":
    main()
