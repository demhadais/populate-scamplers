from collections.abc import Generator
from typing import Any

import httpx

from utils import property_id_map, row_is_empty


def _parse_row(
    row: dict[str, Any], institution_domains: dict[str, str], empty_fn: str
) -> dict[str, Any] | None:
    required_keys = {"name", "email"}
    # This is required because of the Excel formula that creates a person's name
    if row["name"] == " ":
        row["name"] = None

    if row_is_empty(row, required_keys, empty_fn):
        return None

    data = {key: row[key] for key in required_keys}

    email_domain = row["email"].split("@")[-1]
    data["institution_id"] = institution_domains[email_domain]

    microsoft_entra_oid_key = "microsoft_entra_oid"
    if microsoft_entra_oid := row[microsoft_entra_oid_key]:
        data[microsoft_entra_oid_key] = microsoft_entra_oid

    return data


async def _email_domain_institution_map(
    client: httpx.AsyncClient, institution_url: str
) -> dict[str, str]:
    institutions = await client.get(institution_url)
    institutions = institutions.json()
    institution_names = property_id_map("name", institutions)

    institution_domains = {
        "Banner MD Anderson Cancer Center": "mdanderson.org",
        "Cold Spring Harbor Laboratory": "cshl.edu",
        "Houston Methodist": "houstonmethodist.org",
        "Jackson Laboratory": "jax.org",
        "University of Connecticut": "uconn.edu",
        "University of Connecticut Health Center": "uchc.edu",
        "Connecticut Children’s Research Institute": "connecticutchildrens.org",
        "National Institutes of Health": "nih.gov",
        "Yale University": "yale.edu",
        "Pennsylvania State University": "psu.edu",
        "Purdue University": "purdue.edu",
    }

    institution_domains = {
        institution_domains[institution_name]: institution_id
        for institution_name, institution_id in institution_names.items()
    }

    return institution_domains


async def csv_to_new_people(
    client: httpx.AsyncClient,
    institution_url: str,
    people_url: str,
    data: list[dict[str, Any]],
    empty_fn: str,
) -> Generator[dict[str, Any]]:
    institution_domains = await _email_domain_institution_map(client, institution_url)
    new_people = (_parse_row(row, institution_domains, empty_fn) for row in data)
    pre_existing_people = {
        p["email"]
        for p in (await client.get(people_url, params={"limit": 1_000})).json()
    }
    pre_existing_people = pre_existing_people | {
        email.lower() for email in pre_existing_people if email is not None
    }

    new_people = (
        p for p in new_people if not (p is None or p["email"] in pre_existing_people)
    )

    return new_people
