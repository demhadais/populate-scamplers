import asyncio
from datetime import datetime
import logging
from pathlib import Path
from typing import Any
from collections.abc import Generator, Iterable
from uuid import UUID
from scamplepy import ScamplersClient
from scamplepy.common import (
    BiologicalMaterial,
    CellCountingMethod,
    SuspensionMeasurementFields,
    VolumeUnit,
)
from scamplepy.create import NewSuspension, NewSuspensionMeasurement
from scamplepy.query import PersonQuery, SpecimenQuery, SuspensionQuery
from scamplepy.responses import Specimen

from utils import (
    date_str_to_eastcoast_9am,
    get_person_email_id_map,
    row_is_empty,
    str_to_float,
    to_snake_case,
)


def _parse_concentration(
    row: dict[str, Any],
    value_key: str,
    instrument_name: str | None,
    counting_method: str | None,
    measured_at: datetime,
) -> SuspensionMeasurementFields.Concentration | None:
    if value := row[value_key]:
        value = str_to_float(value)
    else:
        return None

    if counting_method is not None:
        parsed_counting_method = CellCountingMethod(to_snake_case(counting_method))
    else:
        parsed_counting_method = None

    unit = (BiologicalMaterial(to_snake_case(row["biological_material"])), VolumeUnit.Millliter)

    return SuspensionMeasurementFields.Concentration(measured_at=measured_at, instrument_name=instrument_name, counting_method=parsed_counting_method, unit=unit, value=value)

def _parse_volume(
    row: dict[str, Any],
    value_key: str,
    measured_at: datetime,
) -> SuspensionMeasurementFields.Volume | None:
    if value := row[value_key]:
        value = str_to_float(value)
    else:
        return None

    return SuspensionMeasurementFields.Volume(measured_at=measured_at, unit=VolumeUnit.Microliter, value=value)


def _parse_suspension_row(
    row: dict[str, Any],
    specimens: dict[str, Specimen],
    people: dict[str, UUID],
    multiplexing_tags: dict[str, UUID],
    for_pool: bool,
) -> NewSuspension | None:
    required_keys = {
        "parent_specimen_readable_id",
        "biological_material",
        "preparer_1_email",
    }
    is_empty = row_is_empty(row, required_keys)

    if is_empty:
        return None

    data = {}
    parent_specimen = specimens[row["parent_specimen_readable_id"]]

    data["parent_specimen_id"] = parent_specimen

    if date_created := row["date_created"]:
        row["created_at"] = date_str_to_eastcoast_9am(date_created)

    if multiplexing_tag_id := row["multiplexing_tag_id"]:
        data["multiplexing_tag_id"] = multiplexing_tags[multiplexing_tag_id]

    data["biological_material"] = BiologicalMaterial(
        to_snake_case(row["biological_material"])
    )

    data["preparer_ids"] = [
        people[row[key]]
        for key in ["preparer_1_email", "preparer_2"]
        if row[key] is not None
    ]

    data["target_cell_recovery"] = str_to_float(row["target_cell_recovery"])

    if lysis_duration := row["lysis_duration_minutes"]:
        data["lysis_duration_minutes"] = str_to_float(lysis_duration)

    data["additional_data"] = {key: row[key] for key in ["experiment_id", "notes"]}
    for key in ["fails_quality_control", "filtered_more_than_once"]:
        if value := row[key]:
            data["additional_data"][key] = bool(value.lower())

    data["measurements"] = []

    if date_created := row["date_created"]:
        measured_at = date_str_to_eastcoast_9am(date_created)
    else:
        measured_at = parent_specimen.info.summary.received_at

    measured_by_for_customer_measurement = parent_specimen.info.submitted_by.id
    measured_by_for_scbl_measurement = data["preparer_ids"][0]

    concentrations = [
        (
            "customer_cell/nucleus_concentration_(cell-nucleus/ml)",
            None,
            None,
            measured_by_for_customer_measurement,
            False
        ),
        (
            "scbl_cell/nucleus_concentration_(cell-nucleus/ml)",
            row["instrument_name"],
            row["counting_method"],
            measured_by_for_scbl_measurement,
            False
        ),
        (
            "scbl_cell/nucleus_concentration_(post-adjustment)_(cell-nucleus/ml)",
            row["instrument_name"],
            row["counting_method"],
            measured_by_for_scbl_measurement,
            False
        ),
        (
            "post-hybridization_cell/nucleus_concentration_(cell-nucleus/ml)",
            row["instrument_name"],
            row["counting_method"],
            measured_by_for_scbl_measurement,
            True
        ),
    ]
    for key, instrument_name, counting_method, measured_by, is_post_hybridization in concentrations:
        if measurement_data := _parse_concentration(row, value_key=key, instrument_name=instrument_name, counting_method=counting_method, measured_at=measured_at)
            measurement = NewSuspensionMeasurement(measured_by=measured_by, data=measurement_data, is_post_hybridization=is_post_hybridization)
            data["measurements"].append(measurement)


    volumes = [
        (
            "customer_volume_(µl)",
            measured_by_for_customer_measurement,
            False,
        ),
        ("scbl_volume_(µl)", measured_by_for_scbl_measurement, False),
        (
            "scbl_volume_(post-adjustment)_(µl)",
            measured_by_for_scbl_measurement,
            False
        ),
        (
            "post-hybridization_volume_(µl)",
            measured_by_for_scbl_measurement,
            True,
        ),
    ]



    viabilities = [
        ("customer_cell_viability_(%)", customer_id, False, customer_measured_at),
        ("scbl_cell_viability_(%)", first_preparer, False, scbl_measured_at),
        (
            "scbl_cell_viability_(post-adjustment)_(%)",
            first_preparer,
            False,
            scbl_measured_at,
        ),
    ]
    for key, measured_by, is_post_hybridization, measured_at in viabilities:
        measurement = NewSuspensionMeasurement(
            measured_by=measured_by,
            is_post_hybridization=is_post_hybdridization,
            data=SuspensionMeasurementFields.Viability(
                measured_at=measured_at,
                value=str_to_float(row[key]),
                instrument_name="unknown",
            ),
        )

    keys = {
        "readable_id",
        "parent_specimen_id",
        "created_at",
        "multiplexing_tag_id",
        "biological_material",
        "preparer_ids",
        "target_cell_recovery",
        "lysis_duration_minutes",
        "measurements",
        "notes",
    }

    return NewSuspension(**{key: val for key, val in row.items() if key in keys})


async def csv_to_new_suspensions(
    client: ScamplersClient, data: list[dict[str, Any]]
) -> Generator[NewSuspension]:
    async with asyncio.TaskGroup() as tg:
        tasks = [
            tg.create_task(task)
            for task in (
                get_person_email_id_map(client),
                client.list_specimens(SpecimenQuery(limit=99_999)),
                client.list_suspensions(SuspensionQuery(limit=99_999)),
                client.list_multiplexing_tags(),
            )
        ]

    people, specimens, pre_existing_suspensions, multiplexing_tags = [
        task.result() for task in tasks
    ]
    specimens = {s.info.summary.readable_id: s for s in specimens}

    new_suspensions = (
        _parse_suspension_row(
            row,
            specimens=specimens,
            people=people,
            multiplexing_tags=multiplexing_tags,
            for_pool=False,
        )
        for row in data
    )

    return (
        susp
        for susp in new_suspensions
        if not (susp is None or susp.readable_id in pre_existing_suspensions)
    )
