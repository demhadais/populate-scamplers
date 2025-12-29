from collections.abc import Generator
from typing import Any

import httpx

from utils import (
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
) -> list[dict[str, Any]] | None:
    required_keys = {"specimen_readable_id", "measured_by"}

    if row_is_empty(row, required_keys, id_key=id_key, empty_fn=empty_fn):
        return None

    specimen = specimens[row["specimen_readable_id"]]

    measurements = []

    measurement = {key: row[key] for key in ["instrument_name"]}
    measured_by = people[row["measured_by"]]
    measurement["measured_by"] = measured_by
    measurement["measured_at"] = (
        date_str_to_eastcoast_9am(row["date_measured"])
        if row["date_measured"]
        else specimen["received_at"]
    )

    if rin := row["rin"]:
        measurement["quantity"] = "RIN"
        measurement["value"] = str_to_float(rin)

    if dv200 := row["dv200"]:
        measurement["quantity"] = "DV200"
        measurement["value"] = str_to_float(dv200)

    if not measurements:
        return None

    return measurements


async def csv_to_new_specimen_measurements(
    client: httpx.AsyncClient,
    specimen_measurement_url: str,
    specimen_url: str,
    people_url: str,
    id_key: str,
    empty_fn: str,
    data: list[dict[str, Any]],
) -> Generator[dict[str, Any]]:
    specimens = (await client.get(specimen_url, params={"limit": 99_999})).json()
    specimen_id_map = {spec["id"]: spec for spec in specimens}

    if len(specimen_id_map) != len(specimens):
        raise ValueError("specimen readable IDs are not unique")

    people = (await client.get(people_url, params={"limit": 99_999})).json()
    people = property_id_map("email", people)

    pre_existing_measurements = (
        await client.get(specimen_measurement_url, params={"limit": 99_999})
    ).json()
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
    return (
        measurement
        for measurement_set in measurements
        if measurement_set is not None
        for measurement in measurement_set
        if measurement not in pre_existing_measurements
    )
