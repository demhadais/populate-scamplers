import asyncio
from collections.abc import Generator
from typing import Any

import httpx

from utils import (
    NO_LIMIT_QUERY,
    date_str_to_eastcoast_9am,
    get_person_email_id_map,
    row_is_empty,
    str_to_bool,
    str_to_float,
    to_snake_case,
)


def _parse_row(
    row: dict[str, Any],
    gem_pools: dict[str, str],
    people: dict[str, str],
    id_key: str,
    empty_fn: str,
) -> dict[str, Any] | None:
    required_keys = {
        "library_type",
        "date_prepared",
        "preparer_email",
        "gems_readable_id",
        "volume_(µl)",
        "n_amplification_cycles",
    }

    if row_is_empty(row, required_keys, id_key=id_key, empty_fn=empty_fn):
        return None

    data = {"readable_id": row["readable_id"]}

    library_type = to_snake_case(row["library_type"])
    library_type = {
        "gene_expression_flex": "gene_expression",
        "vdj-t": "vdj",
        "vdj-b": "vdj",
    }.get(library_type, library_type)
    data["library_type"] = library_type

    data["preparer_ids"] = [
        people[row[key]]
        for key in ["preparer_email", "preparer_2"]
        if row[key] is not None
    ]

    gems_id = gem_pools.get(row["gems_readable_id"])
    if gems_id is None:
        return None
    data["gem_pool_id"] = gems_id

    try:
        data["n_amplification_cycles"] = int(
            str_to_float(row["n_amplification_cycles"])
        )
    except AttributeError:
        if row["n_amplification_cycles"] is None:
            data["n_amplification_cycles"] = 0

    try:
        data["volume_µl"] = int(str_to_float(row["volume_(µl)"]))
    except AttributeError:
        pass

    try:
        data["prepared_at"] = date_str_to_eastcoast_9am(
            row["date_prepared"]
        ).isoformat()
    except ValueError:
        pass

    additional_data = {}
    for key in ["experiment_id", "failure_notes", "storage_location", "notes"]:
        if value := row[key]:
            additional_data[key] = value

    for key in ["is_preamplification_product", "fails_quality_control"]:
        additional_data[key] = str_to_bool(row[key])

    data["additional_data"] = additional_data

    return data


async def csv_to_new_cdna(
    client: httpx.AsyncClient,
    people_url: str,
    gem_pool_url: str,
    cdna_url: str,
    data: list[dict[str, Any]],
    id_key: str,
    empty_fn: str,
) -> Generator[dict[str, Any]]:
    async with asyncio.TaskGroup() as tg:
        people = tg.create_task(get_person_email_id_map(client, people_url))
        gem_pools = tg.create_task(client.get(gem_pool_url, params=NO_LIMIT_QUERY))
        pre_existing_cdna = tg.create_task(client.get(cdna_url, params=NO_LIMIT_QUERY))

    people, gem_pools, pre_existing_cdna = (
        people.result(),
        gem_pools.result().json(),
        pre_existing_cdna.result().json(),
    )

    pre_existing_cdna = {c["readable_id"]: c for c in pre_existing_cdna}

    gem_pools = {pool["readable_id"]: pool["id"] for pool in gem_pools}

    cdna = (
        _parse_row(row, gem_pools, people, id_key=id_key, empty_fn=empty_fn)
        for row in data
    )
    cdna = (c for c in cdna if not (c is None or c["readable_id"] in pre_existing_cdna))

    return cdna
