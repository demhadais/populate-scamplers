import asyncio
from collections.abc import Generator
from typing import Any
from uuid import UUID

from scamplepy import ScamplersClient
from scamplepy.common import (
    MassUnit,
    NucleicAcidConcentration,
    NucleicAcidMeasurementData,
    VolumeUnit,
)
from scamplepy.create import NewLibrary, NewLibraryMeasurement
from scamplepy.query import CdnaQuery, LibraryQuery

from utils import (
    date_str_to_eastcoast_9am,
    get_person_email_id_map,
    property_id_map,
    row_is_empty,
    str_to_bool,
    str_to_float,
)


def _parse_row(
    row: dict[str, Any], cdna: dict[str, UUID], people: dict[str, UUID]
) -> NewLibrary | None:
    required_keys = {
        "cdna_readable_id",
        "number_of_sample_index_pcr_cycles",
        "volume_µl",
        "target_reads_per_cell_(k)",
        "date_prepared",
        "preparer_1_email",
    }

    if row_is_empty(row, required_keys):
        return None

    data = {"readable_id": row["readable_id"]}

    cdna_id = cdna.get(row["cdna_readable_id"])
    if cdna_id is None:
        return None
    data["cdna_id"] = cdna_id

    preparer_ids = [
        people.get(row[k]) for k in ["preparer_1_email", "preparer_2_email"]
    ]
    preparer_ids = [id for id in preparer_ids if id is not None]
    data["preparer_ids"] = preparer_ids

    data["number_of_sample_index_pcr_cycles"] = int(
        str_to_float(row["number_of_sample_index_pcr_cycles"])
    )
    data["volume_mcl"] = str_to_float(row["volume_µl"])
    data["target_reads_per_cell"] = (
        int(str_to_float(row["target_reads_per_cell_(k)"])) * 1000
    )
    data["prepared_at"] = measured_at = date_str_to_eastcoast_9am(row["date_prepared"])

    index_set_name = row["full_index_set_name"]
    if "NA" in index_set_name or "GA" in index_set_name:
        data["single_index_set_name"] = index_set_name
    else:
        data["dual_index_set_name"] = index_set_name

    try:
        measurement1 = NucleicAcidMeasurementData.Electrophoretic(
            measured_at=measured_at,
            instrument_name="Agilent TapeStation",
            mean_size_bp=str_to_float(row["tapestation_mean_library_size_(bp)"]),
            concentration=NucleicAcidConcentration(
                unit=(MassUnit.Picogram, VolumeUnit.Microliter),
                value=str_to_float(row["tapestation_concentration_(pg/µl)"]),
            ),
            sizing_range=tuple(
                int(str_to_float(row[k]))
                for k in (
                    "tapestation_gate_range_minimum_(bp)",
                    "tapestation_gate_range_maximum_(bp)",
                )
            ),
        )
    except Exception:
        measurement1 = None
    try:
        measurement2 = NucleicAcidMeasurementData.Fluorometric(
            measured_at=measured_at,
            instrument_name="ThermoFisher Qubit",
            concentration=NucleicAcidConcentration(
                unit=(MassUnit.Nanogram, VolumeUnit.Microliter),
                value=str_to_float(row["qubit_concentration_(ng/µl)"]),
            ),
        )
    except Exception:
        measurement2 = None

    measurements = [
        NewLibraryMeasurement(measured_by=preparer_ids[0], data=data)
        for data in [measurement1, measurement2]
        if data is not None
    ]
    data["measurements"] = measurements

    additional_data = {}
    for key in ["fails_quality_control"]:
        additional_data[key] = str_to_bool(row[key])

    for key in ["failure_notes", "notes"]:
        if row[key] is not None:
            additional_data[key] = row[key]

    data["additional_data"] = additional_data

    return NewLibrary(**data)


async def csv_to_new_libraries(
    client: ScamplersClient, data: list[dict[str, Any]]
) -> Generator[NewLibrary]:
    async with asyncio.TaskGroup() as tg:
        people = tg.create_task(get_person_email_id_map(client))
        cdna = tg.create_task(client.list_cdna(CdnaQuery(limit=99_999)))
        pre_existing_libraries = tg.create_task(
            client.list_libraries(LibraryQuery(limit=99_999))
        )

    people, cdna, pre_existing_libraries = (
        people.result(),
        cdna.result(),
        pre_existing_libraries.result(),
    )
    cdna = property_id_map("summary.readable_id", "summary.id", cdna)
    pre_existing_libraries = property_id_map(
        "summary.readable_id", "summary.id", pre_existing_libraries
    )

    libraries = (_parse_row(row, cdna, people) for row in data)

    return (
        lib
        for lib in libraries
        if not (lib is None or lib.readable_id in pre_existing_libraries)
    )
