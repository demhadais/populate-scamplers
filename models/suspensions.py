import asyncio
from collections.abc import Generator
from typing import Any
from uuid import UUID

import httpx

from utils import (
    date_str_to_eastcoast_9am,
    get_person_email_id_map,
    row_is_empty,
    str_to_bool,
    str_to_float,
    str_to_int,
    to_snake_case,
)


def _parse_suspension_row(
    row: dict[str, Any],
    specimens: dict[str, dict[str, Any]],
    people: dict[str, UUID],
    multiplexing_tags: dict[str, UUID],
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

    data = {key: row[key] for key in ["readable_id"]}
    parent_specimen = specimens.get(row["parent_specimen_readable_id"])

    if parent_specimen is not None:
        data["parent_specimen_id"] = parent_specimen["id"]

    if date_created := row["date_created"]:
        data["created_at"] = date_str_to_eastcoast_9am(date_created)

    try:
        data["biological_material"] = to_snake_case(row["biological_material"])
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
) -> dict[str, Generator[dict[str, Any]]]:
    async with asyncio.TaskGroup() as tg:
        tasks = [
            tg.create_task(task)
            for task in (
                get_person_email_id_map(client, people_url),
                client.get(specimens_url, params={"limit": 99_999}),
                client.get(suspensions_url, params={"limit": 99_999}),
                client.get(multiplexing_tags_url, params={"limit": 99_999}),
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
            multiplexing_tags=multiplexing_tags,
            id_key=id_key,
            empty_fn=empty_fn,
        )
        for row in data
    )

    new_suspensions = [
        susp
        for susp in new_suspensions
        if not (susp is None or susp["readable_id"] in pre_existing_suspensions)
    ]

    return {
        "cells": (
            susp for susp in new_suspensions if susp["biological_material"] == "cells"
        ),
        "nuclei": (
            susp for susp in new_suspensions if susp["biological_material"] == "nuclei"
        ),
    }
