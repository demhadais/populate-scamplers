import asyncio
from typing import Any
from uuid import UUID
from scamplepy import ScamplersClient
from scamplepy.create import NewInstitution
from pathlib import Path

from utils import read_csv, read_from_cache, write_to_cache


def _parse_institutions(
    data: list[dict[str, Any]],
    already_inserted_institutions: list[NewInstitution],
) -> list[NewInstitution]:
    new_institutions = [
        NewInstitution(id=UUID(row["id"]), name=row["name"]) for row in data
    ]

    return [
        inst for inst in new_institutions if inst not in already_inserted_institutions
    ]


async def create_institutions(
    client: ScamplersClient,
    csv_dir: Path,
    file_renaming: dict[str, str] | None,
    csv_field_renaming: dict[str, str] | None,
    cache_dir: Path,
) -> list[tuple[NewInstitution, Any]]:
    data = read_csv(
        csv_dir,
        "institutions",
        file_renaming=file_renaming,
        field_renaming=csv_field_renaming,
    )

    already_inserted_institutions = read_from_cache(
        cache_dir, "institutions", NewInstitution
    )

    new_institutions = _parse_institutions(
        data,
        already_inserted_institutions,
    )

    created_institutions: list[tuple[NewInstitution, asyncio.Task[Any]]] = []

    async with asyncio.TaskGroup() as tg:
        for new in new_institutions:
            institution = tg.create_task(client.create_institution(new))
            created_institutions.append((new, institution))

    return [(new, task.result()) for new, task in created_institutions]


def write_institutions_to_cache(
    cache_dir: Path, institution_creation_results: list[tuple[NewInstitution, Any]]
):
    write_to_cache(
        cache_dir,
        subdir_name="institutions",
        data=[
            (f"{returned.id}.json", sent)
            for sent, returned in institution_creation_results
        ],
    )
