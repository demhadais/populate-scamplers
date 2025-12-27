import asyncio
from collections.abc import Generator
from typing import Any

import httpx

from utils import (
    get_lab_name_id_map,
    get_person_email_id_map,
    row_is_empty,
)


def _parse_row(row: dict[str, Any], people: dict[str, str], empty_fn: str):
    required_keys = {"name", "pi_email", "delivery_dir"}

    if row_is_empty(row, required_keys, empty_fn):
        return None

    data = {key: row[key] for key in required_keys - {"pi_email"}}
    data["pi_id"] = people[row["pi_email"].lower()]

    return data


async def csv_to_new_labs(
    client: httpx.AsyncClient,
    people_url: str,
    lab_url: str,
    data: list[dict[str, Any]],
    empty_fn: str,
) -> Generator[dict[str, Any]]:
    async with asyncio.TaskGroup() as tg:
        people = tg.create_task(get_person_email_id_map(client, people_url))
        pre_existing_labs = tg.create_task(get_lab_name_id_map(client, lab_url))

    people = people.result()
    pre_existing_labs = pre_existing_labs.result()

    new_labs = (_parse_row(row, people, empty_fn) for row in data)

    new_labs = (
        lab for lab in new_labs if not (lab is None or lab["name"] in pre_existing_labs)
    )

    return new_labs
