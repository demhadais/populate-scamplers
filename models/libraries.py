import asyncio
import uuid
from collections.abc import Generator
from typing import Any

import httpx

from utils import (
    NO_LIMIT_QUERY,
    date_str_to_eastcoast_9am,
    get_person_email_id_map,
    property_id_map,
    row_is_empty,
    str_to_bool,
    str_to_float,
)


def _parse_row(
    row: dict[str, Any],
    cdna: dict[str, str],
    people: dict[str, str],
    id_key: str,
    empty_fn: str,
) -> dict[str, Any] | None:
    required_keys = {
        "cdna_readable_id",
        "number_of_sample_index_pcr_cycles",
        "volume_µl",
        "target_reads_per_cell_(k)",
        "date_prepared",
        "preparer_1_email",
    }

    if row_is_empty(row, required_keys, id_key=id_key, empty_fn=empty_fn):
        return None

    data = {"readable_id": row["readable_id"]}

    data["cdna_id"] = cdna.get(row["cdna_readable_id"], uuid.uuid7())

    data["preparer_ids"] = [
        people[row[k]]
        for k in ["preparer_1_email", "preparer_2_email"]
        if row[k] is not None
    ]

    # These spreadsheets are absolutely infernal
    try:
        data["number_of_sample_index_pcr_cycles"] = int(
            str_to_float(row["number_of_sample_index_pcr_cycles"])
        )
        data["volume_µl"] = int(str_to_float(row["volume_µl"]))
        data["target_reads_per_cell"] = (
            int(str_to_float(row["target_reads_per_cell_(k)"])) * 1000
        )
    except AttributeError:
        pass

    data["prepared_at"] = date_str_to_eastcoast_9am(row["date_prepared"]).isoformat()

    index_set_name = row["full_index_set_name"]
    if "NA" in index_set_name or "GA" in index_set_name:
        data["single_index_set_name"] = index_set_name
    else:
        data["dual_index_set_name"] = index_set_name

    additional_data = {}
    for key in ["fails_quality_control"]:
        additional_data[key] = str_to_bool(row[key])

    for key in ["failure_notes", "notes"]:
        if row[key] is not None:
            additional_data[key] = row[key]

    data["additional_data"] = additional_data

    return data


async def csv_to_new_libraries(
    client: httpx.AsyncClient,
    data: list[dict[str, Any]],
    people_url: str,
    cdna_url: str,
    libraries_url: str,
    id_key: str,
    empty_fn: str,
) -> Generator[dict[str, Any]]:
    async with asyncio.TaskGroup() as tg:
        people = tg.create_task(get_person_email_id_map(client, people_url))
        cdna = tg.create_task(client.get(cdna_url, params=NO_LIMIT_QUERY))
        pre_existing_libraries = tg.create_task(
            client.get(libraries_url, params=NO_LIMIT_QUERY)
        )

    people, cdna, pre_existing_libraries = (
        people.result(),
        cdna.result().json(),
        pre_existing_libraries.result().json(),
    )
    cdna = property_id_map("readable_id", cdna)

    pre_existing_libraries = property_id_map("readable_id", pre_existing_libraries)

    libraries = (
        _parse_row(row, cdna, people, id_key=id_key, empty_fn=empty_fn) for row in data
    )

    return (
        lib
        for lib in libraries
        if not (lib is None or lib["readable_id"] in pre_existing_libraries)
    )
