from pathlib import Path
from typing import Any
from uuid import UUID

from scamplepy.create import NewInstitution
from scamplepy.responses import Institution

from read_write import read_from_cache, write_to_cache


def _parse_new_institutions(
    data: list[dict[str, Any]],
    already_inserted_institutions: list[NewInstitution],
) -> list[NewInstitution]:
    new_institutions = [
        NewInstitution(id=UUID(row["id"]), name=row["name"]) for row in data
    ]

    return [
        inst for inst in new_institutions if inst not in already_inserted_institutions
    ]


async def csv_to_new_institutions(
    data: list[dict[str, Any]],
    cache_dir: Path,
) -> list[NewInstitution]:
    already_inserted_institutions = read_from_cache(
        cache_dir,
        "institutions",
        NewInstitution,
    )

    new_institutions = _parse_new_institutions(
        data,
        already_inserted_institutions,
    )

    return new_institutions


def write_institutions_to_cache(
    cache_dir: Path,
    request_response_pairs: list[tuple[NewInstitution, Institution]],
):
    for request, response in request_response_pairs:
        write_to_cache(
            cache_dir,
            subdir_name="institutions",
            filename=f"{response.id}.json",
            data=request,
        )
