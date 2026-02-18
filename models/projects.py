import asyncio
from collections.abc import Generator
from datetime import UTC, datetime
from typing import Any

import httpx

from utils import (
    get_project_name_id_map,
    row_is_empty,
)


def _parse_row(row: dict[str, Any], id_key: str, empty_fn: str):
    required_keys = {"name"}

    if row_is_empty(row, required_keys, id_key=id_key, empty_fn=empty_fn):
        return None

    data = {key: row[key] for key in required_keys}
    data["started_at"] = datetime(year=2014, month=1, day=1, tzinfo=UTC).isoformat()
    data["ended_at"] = datetime(
        year=2026, month=12, day=31, hour=23, minute=59, second=59, tzinfo=UTC
    ).isoformat()

    return data


async def csv_to_new_projects(
    client: httpx.AsyncClient,
    project_url: str,
    data: list[dict[str, Any]],
    id_key: str,
    empty_fn: str,
) -> Generator[dict[str, Any]]:
    async with asyncio.TaskGroup() as tg:
        pre_existing_projects = tg.create_task(
            get_project_name_id_map(client, project_url)
        )

    pre_existing_projects = pre_existing_projects.result()

    new_projects = (_parse_row(row, id_key=id_key, empty_fn=empty_fn) for row in data)

    new_projects = (
        proj
        for proj in new_projects
        if not (proj is None or proj["name"] in pre_existing_projects)
    )

    return new_projects
