from pathlib import Path
from typing import Any
from uuid import UUID
from scamplepy import ScamplersClient
from scamplepy.create import NewLab
from scamplepy.query import PersonQuery
from scamplepy.responses import Lab

from read_write import read_from_cache, write_to_cache


def _parse_new_labs(
    data: list[dict[str, Any]],
    people: dict[str, UUID],
    already_inserted_labs: list[NewLab],
) -> list[NewLab]:
    new_labs = [
        NewLab(
            name=row["name"],
            pi_id=people[row["pi email"]],
            delivery_dir=row["delivery directory"],
        )
        for row in data
        if row["name"]
    ]

    return [lab for lab in new_labs if lab not in already_inserted_labs]


async def _email_person_map(client: ScamplersClient) -> dict[str, UUID]:
    print(PersonQuery().to_json_string())
    print(PersonQuery().to_base64_json())
    people = await client.list_people(PersonQuery(names=[]))
    people_map = {
        person.info.summary.email: person.info.id_
        for person in people
        if person.info.summary.email is not None
    }

    assert len(people) == len(people_map), "people do not have unique emails"

    return people_map


async def csv_to_lab_creations(
    client: ScamplersClient, data: list[dict[str, Any]], cache_dir: Path
) -> list[NewLab]:
    already_inserted_labs = read_from_cache(cache_dir, "labs", NewLab)

    people = await _email_person_map(client)

    return _parse_new_labs(data, people, already_inserted_labs)


async def write_labs_to_cache(
    cache_dir: Path, request_response_pairs: list[tuple[NewLab, Lab]]
):
    for request, response in request_response_pairs:
        write_to_cache(
            cache_dir,
            subdir_name="labs",
            filename=f"{response.info.id_}.json",
            data=request,
        )
