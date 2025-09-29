from collections.abc import Generator
from typing import Any
from uuid import UUID

from scamplepy import ScamplersClient
from scamplepy.create import NewInstitution
from scamplepy.query import InstitutionQuery

from utils import row_is_empty


def _parse_row(row: dict[str, Any]) -> NewInstitution | None:
    required_keys = {"id", "name"}

    if row_is_empty(row, required_keys):
        return None

    data = {key: row[key] for key in ["name"]}
    data["id"] = UUID(row["id"])

    return NewInstitution(**data)


async def csv_to_new_institutions(
    client: ScamplersClient,
    data: list[dict[str, Any]],
) -> Generator[NewInstitution]:
    pre_existing_institutions = {
        inst.id for inst in await client.list_institutions(InstitutionQuery())
    }
    new_institutions = (_parse_row(row) for row in data)
    new_institutions = (
        inst
        for inst in new_institutions
        if not (inst is None or inst.id in pre_existing_institutions)
    )

    return new_institutions
