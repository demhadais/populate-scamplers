import asyncio
from datetime import datetime
import logging
from typing import Any
from collections.abc import Generator, Mapping
from uuid import UUID
from scamplepy import ScamplersClient
from scamplepy.common import (
    BiologicalMaterial,
    CellCountingMethod,
    SuspensionMeasurementFields,
    VolumeUnit,
)
from scamplepy.create import NewSuspension, NewSuspensionMeasurement
from scamplepy.common import LengthUnit
from scamplepy.query import SpecimenQuery, SuspensionQuery
from scamplepy.responses import MultiplexingTag, Specimen, Suspension

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
    biological_material: BiologicalMaterial | None = None,
) -> SuspensionMeasurementFields.Concentration | None:
    if value := row[value_key]:
        value = str_to_float(value)
    else:
        return None

    if counting_method is not None:
        parsed_counting_method = CellCountingMethod(to_snake_case(counting_method))
    else:
        parsed_counting_method = None

    if biological_material is None:
        biological_material = BiologicalMaterial(
            to_snake_case(row["biological_material"])
        )

    unit = (biological_material, VolumeUnit.Millliter)

    return SuspensionMeasurementFields.Concentration(
        measured_at=measured_at,
        instrument_name=instrument_name,
        counting_method=parsed_counting_method,
        unit=unit,
        value=value,
    )


def _parse_volume(
    row: dict[str, Any],
    value_key: str,
    measured_at: datetime,
) -> SuspensionMeasurementFields.Volume | None:
    if value := row[value_key]:
        value = str_to_float(value)
    else:
        return None

    return SuspensionMeasurementFields.Volume(
        measured_at=measured_at, unit=VolumeUnit.Microliter, value=value
    )


def _parse_viability(
    row: dict[str, Any],
    value_key: str,
    instrument_name: str | None,
    measured_at: datetime,
) -> SuspensionMeasurementFields.Viability | None:
    if value := row[value_key]:
        # Divide by 100 because these values are formatted in a reasonable way (without the percent-sign) so they won't automatically be converted to a decimal inside str_to_float
        value = str_to_float(value) / 100
    else:
        return None

    return SuspensionMeasurementFields.Viability(
        measured_at=measured_at, instrument_name=instrument_name, value=value
    )


def _parse_cell_or_nucleus_diameter(
    row: dict[str, Any],
    value_key: str,
    instrument_name: str | None,
    measured_at: datetime,
) -> SuspensionMeasurementFields.MeanDiameter | None:
    if value := row[value_key]:
        value = str_to_float(value)
    else:
        return None

    biological_material = BiologicalMaterial(to_snake_case(row["biological_material"]))
    unit = (biological_material, LengthUnit.Micrometer)

    return SuspensionMeasurementFields.MeanDiameter(
        measured_at=measured_at, instrument_name=instrument_name, unit=unit, value=value
    )


def _parse_suspension_row(
    row: dict[str, Any],
    specimens: dict[str, Specimen],
    people: dict[str, UUID],
    multiplexing_tags: dict[str, UUID],
    for_pool: bool,
) -> NewSuspension | None:
    if row["readable_id"] == "0":
        return None

    required_keys = {
        "readable_id",
        "parent_specimen_readable_id",
        "biological_material",
        "preparer_1_email",
        "target_cell_recovery",
    }
    is_empty = row_is_empty(row, required_keys)

    if is_empty:
        return None

    data = {key: row[key] for key in ["readable_id"]}

    # Before everything, check the multiplexing tag and `for_pool` to determine whether to actually parse this row
    multiplexing_tag_id = row["multiplexing_tag_id"]
    if (multiplexing_tag_id is None) == for_pool:
        data["multiplexing_tag_id"] = multiplexing_tag_id
    else:
        return None

    try:
        parent_specimen = specimens[row["parent_specimen_readable_id"]]
    except KeyError:
        logging.warning(
            f"skipping {row['readable_id']}: parent specimen {row['parent_specimen_readable_id']} not found"
        )
        return None

    data["parent_specimen_id"] = parent_specimen.info.id_

    if date_created := row["date_created"]:
        row["created_at"] = date_str_to_eastcoast_9am(date_created)

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

    cell_counter = row["cell_counter"]
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
            False,
        ),
        (
            "scbl_cell/nucleus_concentration_(cell-nucleus/ml)",
            cell_counter,
            row["counting_method"],
            measured_by_for_scbl_measurement,
            False,
        ),
        (
            "scbl_cell/nucleus_concentration_(post-adjustment)_(cell-nucleus/ml)",
            cell_counter,
            row["counting_method"],
            measured_by_for_scbl_measurement,
            False,
        ),
        (
            "post-hybridization_cell/nucleus_concentration_(cell-nucleus/ml)",
            cell_counter,
            row["counting_method"],
            measured_by_for_scbl_measurement,
            True,
        ),
    ]
    for (
        key,
        instrument_name,
        counting_method,
        measured_by,
        is_post_hybridization,
    ) in concentrations:
        if measurement_data := _parse_concentration(
            row,
            value_key=key,
            instrument_name=instrument_name,
            counting_method=counting_method,
            measured_at=measured_at,
        ):
            measurement = NewSuspensionMeasurement(
                measured_by=measured_by,
                data=measurement_data,
                is_post_hybridization=is_post_hybridization,
            )
            data["measurements"].append(measurement)

    volumes = [
        (
            "customer_volume_(µl)",
            measured_by_for_customer_measurement,
            False,
        ),
        ("scbl_volume_(µl)", measured_by_for_scbl_measurement, False),
        ("scbl_volume_(post-adjustment)_(µl)", measured_by_for_scbl_measurement, False),
        (
            "post-hybridization_volume_(µl)",
            measured_by_for_scbl_measurement,
            True,
        ),
    ]
    for key, measured_by, is_post_hybridization in volumes:
        if measurement_data := _parse_volume(
            row, value_key=key, measured_at=measured_at
        ):
            measurement = NewSuspensionMeasurement(
                measured_by=measured_by,
                data=measurement_data,
                is_post_hybridization=is_post_hybridization,
            )
            data["measurements"].append(measurement)

    viabilities = [
        (
            "customer_cell_viability_(%)",
            None,
            measured_by_for_customer_measurement,
            False,
        ),
        (
            "scbl_cell_viability_(%)",
            cell_counter,
            measured_by_for_scbl_measurement,
            False,
        ),
        (
            "scbl_cell_viability_(post-adjustment)_(%)",
            cell_counter,
            measured_by_for_scbl_measurement,
            False,
        ),
    ]
    for key, instrument_name, measured_by, is_post_hybridization in viabilities:
        if measurement_data := _parse_viability(
            row, value_key=key, instrument_name=instrument_name, measured_at=measured_at
        ):
            measurement = NewSuspensionMeasurement(
                measured_by=measured_by,
                data=measurement_data,
                is_post_hybridization=is_post_hybridization,
            )
            data["measurements"].append(measurement)

    diameters = [
        (
            "scbl_average_cell/nucleus_diameter_(µm)",
            cell_counter,
            measured_by_for_scbl_measurement,
            False,
        ),
        (
            "scbl_average_cell/nucleus_diameter_(post-adjustment)_(µm)",
            cell_counter,
            measured_by_for_scbl_measurement,
            False,
        ),
        (
            "scbl_post-hybridization_average_cell/nucleus_diameter_(µm)",
            cell_counter,
            measured_by_for_scbl_measurement,
            True,
        ),
    ]
    for key, instrument_name, measured_by, is_post_hybridization in diameters:
        if measurement_data := _parse_cell_or_nucleus_diameter(
            row, value_key=key, instrument_name=instrument_name, measured_at=measured_at
        ):
            measurement = NewSuspensionMeasurement(
                measured_by=measured_by,
                data=measurement_data,
                is_post_hybridization=is_post_hybridization,
            )
            data["measurements"].append(measurement)

    return NewSuspension(**data)


async def csv_to_new_suspensions(
    client: ScamplersClient, data: list[dict[str, Any]], for_pool: bool
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
    specimens = {s.info.summary.readable_id: s for s in specimens}  # pyright: ignore[reportAttributeAccessIssue]
    pre_existing_suspensions = {
        s.info.summary.readable_id
        for s in pre_existing_suspensions  # pyright: ignore[reportAttributeAccessIssue]
    }

    new_suspensions = (
        _parse_suspension_row(
            row,
            specimens=specimens,  # pyright: ignore[reportArgumentType]
            people=people,  # pyright: ignore[reportArgumentType]
            multiplexing_tags=multiplexing_tags,  # pyright: ignore[reportArgumentType]
            for_pool=False,
        )
        for row in data
    )

    return (
        susp
        for susp in new_suspensions
        if not (susp is None or susp.readable_id in pre_existing_suspensions)
    )
