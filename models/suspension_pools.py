from collections.abc import Generator
from typing import Any
from uuid import UUID
from scamplepy import ScamplersClient
from scamplepy.create import (
    NewSuspension,
    NewSuspensionPool,
    NewSuspensionPoolMeasurement,
)

from models.suspensions import (
    csv_to_new_suspensions,
    _parse_concentration,
    _parse_viability,
    _parse_volume,
    _parse_cell_or_nucleus_diameter,
    _row_is_ocm,
)
from utils import date_str_to_eastcoast_9am, row_is_empty


def _parse_row(
    row: dict[str, Any],
    suspensions: dict[str, list[NewSuspension]],
    people: dict[str, UUID],
    pool_to_gems: dict[str, dict[str, Any]],
) -> NewSuspensionPool | None:
    required_keys = {"readable_id", "name", "date_pooled"}

    if row_is_empty(row, required_keys):
        return None

    data = {key: row[key] for key in required_keys - {"date_pooled"}}

    # Assign basic information
    data["pooled_at"] = pooled_at = date_str_to_eastcoast_9am(row["date_pooled"])
    data["suspensions"] = child_suspensions = suspensions[data["readable_id"]]
    data["preparer_ids"] = preparer_ids = [
        people[row[email_key]]
        for email_key in ["preparer_1_email", "preparer_2"]
        if row[email_key] is not None
    ]

    # Prepare the necessary data to construct a concentration
    data["measurements"] = []

    biological_material = child_suspensions[0].biological_material

    readable_id = data["readable_id"]
    gems = pool_to_gems[readable_id]
    chip_run_on = date_str_to_eastcoast_9am(gems["date_chip_run"])

    measured_by = preparer_ids[0]

    concentrations = [
        ("pre-storage_cell/nucleus_concentration_(cell-nucleus/ml)", pooled_at),
        ("cell/nucleus_concentration_(cell-nucleus/ml)", chip_run_on),
    ]
    for key, measured_at in concentrations:
        if measurement_data := _parse_concentration(
            row, key, biological_material=biological_material, measured_at=measured_at
        ):
            data["measurements"].append(
                NewSuspensionPoolMeasurement(
                    measured_by=measured_by, data=measurement_data
                )
            )

    volumes = [
        ("pre-storage_volume_(µl)", pooled_at),
        ("volume_(µL)", chip_run_on),
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
        row, value_key="average_cell/nucleus_diameter_(µm)", measured_at=chip_run_on
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
        if (pooled_into_id := suspension_row["pooled_into_id"]) and not _row_is_ocm(
            suspension_row
        ):
            grouped_suspensions[pooled_into_id].append(
                suspensions_by_readable_id[suspension_row["readable_id"]]
            )
    ...
