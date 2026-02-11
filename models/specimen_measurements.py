import asyncio
from collections.abc import Callable, Generator
from typing import Any

import httpx

from utils import (
    NO_LIMIT_QUERY,
    date_str_to_eastcoast_9am,
    property_id_map,
    row_is_empty,
    str_to_float,
)


def _parse_specimen_measurement_row(
    row: dict[str, Any],
    people: dict[str, Any],
    specimens: dict[str, dict[str, Any]],
    id_key: str,
    empty_fn: str,
) -> tuple[str, list[dict[str, Any]]] | None:
    required_keys = {"specimen_readable_id", "measured_by"}

    if row_is_empty(row, required_keys, id_key=id_key, empty_fn=empty_fn):
        return None

    specimen = specimens.get(row["specimen_readable_id"])
    if specimen is None:
        return None

    measurements = []
    for column_name, measurement_quantity in [("rin", "RIN"), ("dv200", "DV200")]:
        measurement = {key: row[key] for key in ["instrument_name"]}
        if measured_by := people.get(row["measured_by"]):
            measurement["measured_by"] = measured_by

        measurement["measured_at"] = (
            date_str_to_eastcoast_9am(row["date_measured"])
            if row["date_measured"]
            else specimen["received_at"]
        )

        if value := row[column_name]:
            if value != " ":
                measurement["data"] = {
                    "quantity": measurement_quantity,
                    "value": str_to_float(value),
                }
                measurements.append(measurement)

    if not measurements:
        return None

    return (specimen["id"], measurements)


async def _get_pre_existing_measurements(
    client: httpx.AsyncClient,
    specimen_ids: list[str],
    specimen_measurement_url_creator: Callable[[str], str],
) -> list[dict[str, Any]]:
    tasks = []

    async with asyncio.TaskGroup() as tg:
        for specimen_id in specimen_ids:
            url = specimen_measurement_url_creator(specimen_id)
            tasks.append(tg.create_task(client.get(url)))

    return [m for task in tasks for m in task.result().json()]


async def csv_to_new_specimen_measurements(
    client: httpx.AsyncClient,
    specimen_url: str,
    people_url: str,
    specimen_measurement_url_creator: Callable[[str], str],
    id_key: str,
    empty_fn: str,
    data: list[dict[str, Any]],
) -> Generator[tuple[str, dict[str, Any]]]:
    specimens = (await client.get(specimen_url, params=NO_LIMIT_QUERY)).json()
    specimen_id_map = {spec["readable_id"]: spec for spec in specimens}

    if len(specimen_id_map) != len(specimens):
        raise ValueError("specimen readable IDs are not unique")

    people = (await client.get(people_url, params=NO_LIMIT_QUERY)).json()
    people = property_id_map("email", people)

    pre_existing_measurements = await _get_pre_existing_measurements(
        client, [sp["id"] for sp in specimens], specimen_measurement_url_creator
    )

    for m in pre_existing_measurements:
        # delete the measurement ID so that the parsed measurement row can compare to the pre-existing measurments
        del m["id"]

    measurements = (
        _parse_specimen_measurement_row(
            row,
            people=people,
            specimens=specimen_id_map,
            id_key=id_key,
            empty_fn=empty_fn,
        )
        for row in data
    )

    measurements = (m for m in measurements if m is not None)

    return (
        (specimen_id, measurement)
        for specimen_id, measurement_set in measurements
        for measurement in measurement_set
        if measurement not in pre_existing_measurements
    )
