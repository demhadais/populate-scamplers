import asyncio
from collections.abc import Generator
from typing import Any
from uuid import UUID

from models.suspensions import csv_to_new_suspensions
from utils import date_str_to_eastcoast_9am, get_person_email_id_map, row_is_empty


def _parse_row(
    row: dict[str, Any],
    suspensions: dict[str, list[dict[str, Any]]],
    people: dict[str, UUID],
    multiplexing_tags: dict[str, UUID],
    pool_to_gems: dict[str, dict[str, Any]],
    id_key: str,
    empty_fn: str,
) -> dict[str, Any] | None:
    required_keys = {"readable_id", "name", "date_pooled"}

    if row_is_empty(row, required_keys, id_key=id_key, empty_fn=empty_fn):
        return None

    data = {key: row[key] for key in required_keys - {"date_pooled"}}

    # Assign basic information
    data["pooled_at"] = pooled_at = date_str_to_eastcoast_9am(row["date_pooled"])
    data["suspensions"] = child_suspensions = [
        {
            "suspension_id": susp["id"],
            "tag_id": multiplexing_tags[susp["multiplexing_tag_id"]],
        }
        for susp in suspensions[data["readable_id"]]
        if "ob" not in susp["multiplexing_tag_id"].lower()
    ]

    if not child_suspensions:
        return None

    data["preparer_ids"] = preparer_ids = [
        people[row[email_key]]
        for email_key in ["preparer_1_email", "preparer_2"]
        if row[email_key] is not None
    ]

    # Prepare the necessary data to construct a concentration
    data["measurements"] = []
    suspension_content = child_suspensions[0]["content"]

    readable_id = data["readable_id"]

    # If no GEMs pool was found, it just means it hasn't been run
    try:
        gems = pool_to_gems[readable_id]
        chip_run_on = date_str_to_eastcoast_9am(gems["date_chip_run"])
    except KeyError:
        chip_run_on = pooled_at
        pass

    measured_by = preparer_ids[0]

    concentrations = [
        ("pre-storage_cell/nucleus_concentration_(cell-nucleus/ml)", pooled_at),
        ("cell/nucleus_concentration_(cell-nucleus/ml)", chip_run_on),
    ]
    for key, measured_at in concentrations:
        if measurement_data := _parse_concentration(
            row, key, biological_material=suspension_content, measured_at=measured_at
        ):
            data["measurements"].append(
                NewSuspensionPoolMeasurement(
                    measured_by=measured_by, data=measurement_data
                )
            )

    volumes = [
        ("pre-storage_volume_(µl)", pooled_at),
        ("volume_(µl)", chip_run_on),
    ]
    for key, measured_at in volumes:
        if measurement_data := _parse_volume(
            row, value_key=key, measured_at=measured_at
        ):
            data["measurements"].append(
                NewSuspensionPoolMeasurement(
                    measured_by=measured_by, data=measurement_data
                )
            )

    # Viability is only measured after storage (or if there was no storage at all)
    if measurement_data := _parse_viability(
        row, value_key="cell_viability_(%)", measured_at=chip_run_on
    ):
        data["measurements"].append(
            NewSuspensionPoolMeasurement(measured_by=measured_by, data=measurement_data)
        )

    # Same with cell/nucleus diameter
    if measurement_data := _parse_cell_or_nucleus_diameter(
        row,
        value_key="average_cell/nucleus_diameter_(µm)",
        biological_material=suspension_content,
        measured_at=chip_run_on,
    ):
        data["measurements"].append(
            NewSuspensionPoolMeasurement(measured_by=measured_by, data=measurement_data)
        )

    return NewSuspensionPool(**data)


async def csvs_to_new_suspension_pools(
    client: ScamplersClient,
    suspension_pool_data: list[dict[str, Any]],
    suspension_data: list[dict[str, Any]],
    gems_data: list[dict[str, Any]],
    gems_loading_data: list[dict[str, Any]],
) -> Generator[NewSuspensionPool]:
    async with asyncio.TaskGroup() as tg:
        tasks = [
            tg.create_task(task)
            for task in (
                get_person_email_id_map(client),
                client.list_suspension_pools(SuspensionPoolQuery(limit=99_999)),
            )
        ]

    people, pre_existing_suspension_pools = [task.result() for task in tasks]
    pre_existing_suspension_pools = {
        pool.summary.readable_id  # pyright: ignore[reportAttributeAccessIssue]
        for pool in pre_existing_suspension_pools
    }

    pooled_suspensions = await csv_to_new_suspensions(
        client, suspension_data, for_pool=True
    )
    suspensions_by_readable_id = {susp.readable_id: susp for susp in pooled_suspensions}
    grouped_suspensions = {
        row["readable_id"]: []
        for row in suspension_pool_data
        if row["readable_id"] is not None
    }
    for suspension_row in suspension_data:
        if suspension_row["readable_id"] not in suspensions_by_readable_id:
            continue

        if (pooled_into_id := suspension_row["pooled_into_id"]) and not _row_is_ocm(
            suspension_row
        ):
            grouped_suspensions[pooled_into_id].append(
                suspensions_by_readable_id[suspension_row["readable_id"]]
            )

    pool_to_gems = {}
    for gems_loading_row in gems_loading_data:
        gems_id = gems_loading_row["gems_readable_id"]
        suspension_pool_readable_id = gems_loading_row["suspension_pool_readable_id"]
        if suspension_pool_readable_id is None:
            continue

        for gems_row in gems_data:
            if gems_row["readable_id"] != gems_id:
                continue

            pool_to_gems[suspension_pool_readable_id] = gems_row

    new_suspension_pools = (
        _parse_row(
            row,
            suspensions=grouped_suspensions,
            people=people,  # pyright: ignore[reportArgumentType]
            pool_to_gems=pool_to_gems,
        )
        for row in suspension_pool_data
    )
    new_suspension_pools = (
        pool
        for pool in new_suspension_pools
        if not (pool is None or pool.readable_id in pre_existing_suspension_pools)
    )
    return new_suspension_pools
    ...
