import asyncio
from collections.abc import Generator
from typing import Any

import httpx
from utils import (
    NO_LIMIT_QUERY,
    date_str_to_eastcoast_9am,
    get_person_email_id_map,
    row_is_empty,
)


def _parse_row(
    row: dict[str, Any],
    suspensions: dict[str, list[dict[str, Any]]],
    people: dict[str, str],
    multiplexing_tags: dict[str, str],
    id_key: str,
    empty_fn: str,
) -> dict[str, Any] | None:
    required_keys = {"readable_id", "name", "date_pooled"}

    if row_is_empty(row, required_keys, id_key=id_key, empty_fn=empty_fn):
        return None

    data = {key: row[key] for key in required_keys}

    # Assign basic information
    try:
        data["pooled_at"] = date_str_to_eastcoast_9am(row["date_pooled"])
    except TypeError:
        pass

    data["suspensions"] = child_suspensions = [
        {
            "suspension_id": susp["id"],
            "tag_id": multiplexing_tags.get(susp["multiplexing_tag_id"]),
        }
        for susp in suspensions[data["readable_id"]]
        if susp["multiplexing_tag_id"] is not None
        and "ob" not in susp["multiplexing_tag_id"].lower()
    ]

    if not child_suspensions:
        return None

    data["preparer_ids"] = [
        people[row[email_key]]
        for email_key in ["preparer_1_email", "preparer_2"]
        if row[email_key] is not None
    ]

    return data


async def csvs_to_new_suspension_pools(
    client: httpx.AsyncClient,
    people_url: str,
    suspension_pool_url: str,
    suspensions_url: str,
    multiplexing_tags_url: str,
    suspension_pool_data: list[dict[str, Any]],
    suspension_csv_data: list[dict[str, Any]],
    id_key: str,
    empty_fn: str,
) -> Generator[dict[str, Any]]:
    async with asyncio.TaskGroup() as tg:
        tasks = (
            tg.create_task(get_person_email_id_map(client, people_url)),
            tg.create_task(client.get(suspension_pool_url, params=NO_LIMIT_QUERY)),
            tg.create_task(client.get(suspensions_url, params=NO_LIMIT_QUERY)),
            tg.create_task(client.get(multiplexing_tags_url)),
        )

    people, pre_existing_suspension_pools, suspensions_from_api, multiplexing_tags = (
        tasks[0].result(),
        tasks[1].result(),
        tasks[2].result(),
        tasks[3].result(),
    )

    pre_existing_suspension_pools = pre_existing_suspension_pools.json()
    pre_existing_suspension_pools = {
        pool["readable_id"] for pool in pre_existing_suspension_pools
    }

    grouped_suspensions = {
        row["readable_id"]: []
        for row in suspension_pool_data
        if row["readable_id"] is not None
    }
    suspension_csv_data = {
        susp_row["readable_id"]: susp_row for susp_row in suspension_csv_data
    }  # pyright: ignore[reportAssignmentType]

    suspensions_from_api = suspensions_from_api.json()
    for suspension in suspensions_from_api:
        readable_id = suspension["readable_id"]
        suspension_from_csv = suspension_csv_data[readable_id]
        if pooled_into := suspension_from_csv["pooled_into_id"]:
            suspension["pooled_into_id"] = pooled_into

            multiplexing_tag = suspension_from_csv["multiplexing_tag_id"]
            suspension["multiplexing_tag_id"] = multiplexing_tag

            pooled_suspension_list = grouped_suspensions[pooled_into]
            pooled_suspension_list.append(suspension)

    multiplexing_tags = multiplexing_tags.json()
    multiplexing_tags = {tag["tag_id"]: tag["id"] for tag in multiplexing_tags}
    new_suspension_pools = (
        _parse_row(
            row,
            suspensions=grouped_suspensions,
            people=people,
            multiplexing_tags=multiplexing_tags,
            id_key=id_key,
            empty_fn=empty_fn,
        )
        for row in suspension_pool_data
    )
    new_suspension_pools = (
        pool
        for pool in new_suspension_pools
        if not (pool is None or pool["readable_id"] in pre_existing_suspension_pools)
    )
    return new_suspension_pools
