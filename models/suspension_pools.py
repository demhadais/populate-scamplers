from collections.abc import Generator, Iterable
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
)
from utils import date_str_to_eastcoast_9am, row_is_empty


def _parse_row(
    row: dict[str, Any],
    suspensions: dict[str, list[NewSuspension]],
    people: dict[str, UUID],
) -> NewSuspensionPool | None:
    required_keys = {"readable_id", "name", "date_pooled"}

    if row_is_empty(row, required_keys):
        return None

    data = {key: row[key] for key in required_keys - {"date_pooled"}}
    data["pooled_at"] = pooled_at = date_str_to_eastcoast_9am(row["date_pooled"])

    data["suspensions"] = child_suspensions = suspensions[data["readable_id"]]
    biological_material = child_suspensions[0].biological_material

    concentrations = [
        ("pre-storage_cell/nucleus_concentration_(cell-nucleus/ml)", None, None, biological_material, False),
        ("cell/nucleus_concentration_(cell-nucleus/ml)", None, None, biological_material, True),
    ]

    return NewSuspensionPool()


async def csvs_to_new_suspension_pools(
    client: ScamplersClient,
    suspension_pool_data: list[dict[str, Any]],
    suspension_data: list[dict[str, Any]],
) -> Generator[NewSuspensionPool]:
    new_suspensions = await csv_to_new_suspensions(
        client, suspension_data, for_pool=True
    )
    ...
