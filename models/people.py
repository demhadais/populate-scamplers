from pathlib import Path
from typing import Any
from uuid import UUID
from scamplepy import ScamplersClient
from scamplepy.create import NewPerson
from scamplepy.query import InstitutionQuery
from scamplepy.responses import Person

from read_write import read_from_cache, write_to_cache


def _parse_new_people(
    data: list[dict[str, Any]],
    institution_domains: dict[str, UUID],
    already_inserted_people: list[NewPerson],
) -> list[NewPerson]:
    for row in data:
        if row.get("email_domain") is not None:
            raise ValueError("'email_domain' should not be a field in people data")

        row["email_domain"] = row["email"].split("@")[-1]

    new_people = [
        NewPerson(
            name=row["name"],
            institution_id=institution_domains[row["email_domain"]],
            email=row["email"],
            ms_user_id=UUID(row["ms_user_id"]) if row["ms_user_id"] else None,
        )
        for row in data
        if row["email"]
    ]

    return [p for p in new_people if p not in already_inserted_people]


async def _email_domain_institution_map(client: ScamplersClient) -> dict[str, UUID]:
    institutions = await client.list_institutions(InstitutionQuery())
    institution_names = {
        institution.name: institution.id for institution in institutions
    }

    assert len(institutions) == len(institution_names), (
        "institutions do not have unique names"
    )

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
    }

    institution_domains = {
        institution_domains[institution_name]: institution_id
        for institution_name, institution_id in institution_names.items()
    }

    return institution_domains


async def csv_to_person_creations(
    client: ScamplersClient,
    data: list[dict[str, Any]],
    cache_dir: Path,
) -> list[NewPerson]:
    already_inserted_people = read_from_cache(
        cache_dir,
        "people",
        NewPerson,
    )

    return _parse_new_people(
        data,
        institution_domains=await _email_domain_institution_map(client),
        already_inserted_people=already_inserted_people,
    )


def write_people_to_cache(
    cache_dir: Path,
    request_response_pairs: list[tuple[NewPerson, Person]],
):
    for request, response in request_response_pairs:
        if not request or not response:
            continue

        write_to_cache(
            cache_dir,
            subdir_name="people",
            filename=f"{response.info.id_}.json",
            data=request,
        )
