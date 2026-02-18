import asyncio
from collections.abc import Generator
from typing import Any
from uuid import UUID

import httpx

from utils import (
    NO_LIMIT_QUERY,
    date_str_to_eastcoast_9am,
    get_person_email_id_map,
    row_is_empty,
    str_to_bool,
    str_to_float,
    str_to_int,
    to_snake_case,
)


def _map_bad_specimens(row: dict[str, Any]) -> dict[str, Any]:
    map = {
        "25SP1819": "25SP1794",
        "25SP1820": "25SP1795",
        "25SP1821": "25SP1796",
        "25SP1822": "25SP1797",
        "25SP1823": "25SP1798",
        "25SP1824": "25SP1799",
        "25SP1825": "25SP1800",
        "25SP1826": "25SP1801",
    }

    if mapped_parent_specimen_id := map.get(row["parent_specimen_readable_id"]):
        row["parent_specimen_readable_id"] = mapped_parent_specimen_id

    return row


def _parse_suspension_row(
    row: dict[str, Any],
    specimens: dict[str, dict[str, Any]],
    people: dict[str, UUID],
    id_key: str,
    empty_fn: str,
) -> dict[str, Any] | None:
    required_keys = {
        "readable_id",
        "parent_specimen_readable_id",
        "biological_material",
        "preparer_1_email",
        "target_cell_recovery",
    }
    is_empty = row_is_empty(row, required_keys, id_key=id_key, empty_fn=empty_fn)

    if is_empty:
        return None

    row = _map_bad_specimens(row)

    data = {key: row[key] for key in ["readable_id"]}

    parent_specimen = specimens.get(row["parent_specimen_readable_id"])

    if parent_specimen is not None:
        data["parent_specimen_id"] = parent_specimen["id"]

    if date_created := row["date_created"]:
        data["created_at"] = date_str_to_eastcoast_9am(date_created)

    try:
        data["content"] = to_snake_case(row["biological_material"])
    except AttributeError:
        raise ValueError(f"no biological material supplied for {data['readable_id']}")

    data["preparer_ids"] = [
        people[row[key]]
        for key in ["preparer_1_email", "preparer_2"]
        if row[key] is not None
    ]

    if target_cell_recovery := row["target_cell_recovery"]:
        data["target_cell_recovery"] = str_to_int(target_cell_recovery)

    if lysis_duration := row["lysis_duration_minutes"]:
        data["lysis_duration_minutes"] = str_to_float(lysis_duration)

    data["additional_data"] = {key: row[key] for key in ["experiment_id", "notes"]}
    for key in ["fails_quality_control", "filtered_more_than_once"]:
        data["additional_data"][key] = str_to_bool(row[key])

    return data


async def csv_to_new_suspensions(
    client: httpx.AsyncClient,
    people_url: str,
    specimens_url: str,
    suspensions_url: str,
    multiplexing_tags_url: str,
    data: list[dict[str, Any]],
    id_key: str,
    empty_fn: str,
) -> Generator[dict[str, Any]]:
    async with asyncio.TaskGroup() as tg:
        tasks = [
            tg.create_task(task)
            for task in (
                get_person_email_id_map(client, people_url),
                client.get(specimens_url, params=NO_LIMIT_QUERY),
                client.get(suspensions_url, params=NO_LIMIT_QUERY),
                client.get(multiplexing_tags_url, params=NO_LIMIT_QUERY),
            )
        ]

    people, specimens, pre_existing_suspensions, multiplexing_tags = [
        task.result() for task in tasks
    ]
    specimens, pre_existing_suspensions, multiplexing_tags = [
        response.json()  # pyright: ignore[reportAttributeAccessIssue]
        for response in [specimens, pre_existing_suspensions, multiplexing_tags]
    ]
    specimens = {s["readable_id"]: s for s in specimens}
    pre_existing_suspensions = {s["readable_id"] for s in pre_existing_suspensions}
    multiplexing_tags = {tag["tag_id"]: tag["id"] for tag in multiplexing_tags}

    new_suspensions = (
        _parse_suspension_row(
            row,
            specimens=specimens,  # pyright: ignore[reportArgumentType]
            people=people,  # pyright: ignore[reportArgumentType]
            id_key=id_key,
            empty_fn=empty_fn,
        )
        for row in data
    )

    new_suspensions = (
        susp
        for susp in new_suspensions
        if not (susp is None or susp["readable_id"] in pre_existing_suspensions)
    )

    return new_suspensions
