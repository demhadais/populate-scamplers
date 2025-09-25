from typing import Any
from uuid import UUID
from scamplepy import ScamplersClient
from scamplepy.create import NewPerson
from scamplepy.query import InstitutionQuery, PersonQuery

from utils import property_id_map, row_is_empty


def _parse_row(
    row: dict[str, Any], institution_domains: dict[str, UUID]
) -> NewPerson | None:
    required_keys = {"name", "email"}
    # This is required because of the Excel formula that creates a person's name
    if row["name"] == " ":
        row["name"] = None

    if row_is_empty(row, required_keys):
        return None

    data = {key: row[key] for key in required_keys}

    email_domain = row["email"].split("@")[-1]
    data["institution_id"] = institution_domains[email_domain]
    data["ms_user_id"] = UUID(row["ms_user_id"]) if row["ms_user_id"] else None

    return NewPerson(**data)


async def _email_domain_institution_map(client: ScamplersClient) -> dict[str, UUID]:
    institutions = await client.list_institutions(InstitutionQuery())
    institution_names = property_id_map("name", "id", institutions)

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
    }

    institution_domains = {
        institution_domains[institution_name]: institution_id
        for institution_name, institution_id in institution_names.items()
    }

    return institution_domains


async def csv_to_new_people(
    client: ScamplersClient,
    data: list[dict[str, Any]],
) -> list[NewPerson]:
    institution_domains = await _email_domain_institution_map(client)
    new_people = (_parse_row(row, institution_domains) for row in data)
    pre_existing_people = {
        p.info.summary.email for p in await client.list_people(PersonQuery(limit=9_999))
    }
    pre_existing_people = pre_existing_people | {
        email.lower() for email in pre_existing_people if email is not None
    }

    new_people = [
        p for p in new_people if not (p is None or p.email in pre_existing_people)
    ]

    return new_people
