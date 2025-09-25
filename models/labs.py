from typing import Any
from uuid import UUID
from scamplepy import ScamplersClient
from scamplepy.create import NewLab
from scamplepy.query import LabQuery, PersonQuery

from utils import property_id_map, row_is_empty


def _parse_row(row: dict[str, Any], people: dict[str, UUID]):
    required_keys = {"name", "pi_email", "delivery_dir"}

    if row_is_empty(row, required_keys):
        return None

    data = {key: row[key] for key in required_keys - {"pi_email"}}
    data["pi_id"] = people[row["pi_email"]]

    return NewLab(**data)


async def csv_to_new_labs(
    client: ScamplersClient, data: list[dict[str, Any]]
) -> list[NewLab]:
    people = await client.list_people(PersonQuery())
    people = property_id_map("info.summary.email", "info.id_", people)
    people = people | {email.lower(): people[email] for email in people}

    new_labs = (_parse_row(row, people) for row in data)
    pre_existing_labs = {
        lab.info.summary.name for lab in await client.list_labs(LabQuery(limit=9_999))
    }

    new_labs = [
        lab for lab in new_labs if not (lab is None or lab.name in pre_existing_labs)
    ]

    return new_labs
