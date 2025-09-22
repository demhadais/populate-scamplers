import logging
from pathlib import Path
from typing import Any
from uuid import UUID
from scamplepy import ScamplersClient
from scamplepy.common import (
    BiologicalMaterial,
    CellCountingMethod,
    SuspensionMeasurementFields,
    VolumeUnit,
)
from scamplepy.create import NewSuspension, NewSuspensionMeasurement
from scamplepy.query import PersonQuery, SpecimenQuery
from scamplepy.responses import Specimen

from read_write import (
    eastcoast_9am_from_date_str,
    property_id_map,
    row_is_empty,
    to_snake_case,
)


def _parse_suspension_row(
    row: dict[str, Any],
    specimens: dict[str, Specimen],
    people: dict[str, UUID],
    multiplexing_tags: dict[str, UUID],
) -> NewSuspension | None:
    required_keys = {
        "parent_specimen_readable_id",
        "biological_material",
        "preparer_1_email",
    }
    is_empty = row_is_empty(row, required_keys)

    if is_empty:
        return None

    parent_specimen = specimens[row["parent_specimen_readable_id"]]

    row["parent_specimen_id"] = parent_specimen.info.id_

    if date_created := row["date_created"]:
        row["created_at"] = eastcoast_9am_from_date_str(date_created)

    if multiplexing_tag_id := row["multiplexing_tag_id"]:
        row["multiplexing_tag_id"] = multiplexing_tags[multiplexing_tag_id]

    row["biological_material"] = BiologicalMaterial(
        to_snake_case(row["biological_material"])
    )

    row["preparer_ids"] = [
        people[row[key]]
        for key in ["preparer_1_email", "preparer_2"]
        if row[key] is not None
    ]

    row["target_cell_recovery"] = float(row["target_cell_recovery"].replace(",", ""))

    if lysis_duration := row["lysis_duration_minutes"]:
        row["lysis_duration_minutes"] = float(lysis_duration)

    row["notes"] = "; ".join(
        row[key]
        for key in [
            "notes",
        ]
        if row[key] is not None
    )

    row["measurements"] = []

    # If something goes wrong in trying to get the cell-counting method, we'll just get an error when we try to add it to a measurement. If we don't try to add it to a measurement, then it doesn't matter and it can just stay as None
    try:
        cell_counting_method = CellCountingMethod(to_snake_case(row["counting_method"]))
    except Exception:
        cell_counting_method = None

    customer_id = parent_specimen.info.submitted_by.id
    customer_measured_at = parent_specimen.info.summary.received_at
    customer_instrument_name = "unknown"

    first_preparer = row["preparer_ids"][0]
    scbl_measured_at = row.get("created_at", parent_specimen.info.summary.received_at)

    concentrations = [
        (
            "customer_cell/nucleus_concentration_(cell-nucleus/ml)",
            customer_id,
            False,
            customer_measured_at,
            customer_instrument_name,
            cell_counting_method,
        ),
        (
            "scbl_cell/nucleus_concentration_(cell-nucleus/ml)",
            first_preparer,
            False,
            scbl_measured_at,
            row["cell_counter"],
            cell_counting_method,
        ),
        (
            "scbl_cell/nucleus_concentration_(post-adjustment)_(cell-nucleus/ml)",
            first_preparer,
            False,
            scbl_measured_at,
            row["cell_counter"],
            cell_counting_method,
        ),
        (
            "post-hybridization_cell/nucleus_concentration_(cell-nucleus/ml)",
            first_preparer,
            True,
            scbl_measured_at,
            row["cell_counter"],
            cell_counting_method,
        ),
    ]

    for (
        key,
        measured_by,
        is_post_hybdridization,
        measured_at,
        cell_counter,
        cell_counting_method,
    ) in (c for c in concentrations if row[c[0]] is not None):
        measurement = NewSuspensionMeasurement(
            measured_by=measured_by,
            is_post_hybridization=is_post_hybdridization,
            data=SuspensionMeasurementFields.Concentration(
                measured_at=measured_at,
                instrument_name=cell_counter,
                counting_method=cell_counting_method,
                value=float(row[key].replace(",", "")),
                unit=(row["biological_material"], VolumeUnit.Millliter),
            ),
        )
        row["measurements"].append(measurement)

    volumes = [
        (
            "customer_volume_(µl)",
            customer_id,
            False,
            customer_measured_at,
        ),
        ("scbl_volume_(µl)", first_preparer, False, scbl_measured_at),
        (
            "scbl_volume_(post-adjustment)_(µl)",
            first_preparer,
            False,
            scbl_measured_at,
        ),
        (
            "post-hybridization_volume_(µl)",
            first_preparer,
            True,
            scbl_measured_at,
        ),
    ]

    for key, measured_by, is_post_hybdridization, measured_at in (
        v for v in volumes if row[v[0]] is not None
    ):
        measurement = NewSuspensionMeasurement(
            measured_by=measured_by,
            is_post_hybridization=is_post_hybdridization,
            data=SuspensionMeasurementFields.Volume(
                measured_at=measured_at,
                value=float(row[key].replace(",", "")),
                unit=VolumeUnit.Microliter,
            ),
        )
        row["measurements"].append(measurement)

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
    client: ScamplersClient, csv: list[dict[str, Any]], cache_dir: Path
) -> list[NewSuspension]:
    people = await client.list_people(PersonQuery())
    people = property_id_map("info.summary.email", "info.id_", people)

    specimens = await client.list_specimens(SpecimenQuery(limit=99_999))
    specimens = {s.info.summary.readable_id: s for s in specimens}

    multiplexing_tags = await client.list_multiplexing_tags()
    multiplexing_tags = {t.tag_id: t.id for t in multiplexing_tags}

    suspensions = []
    for row in csv:
        try:
            if suspension := _parse_suspension_row(
                row,
                specimens=specimens,
                people=people,
                multiplexing_tags=multiplexing_tags,
            ):
                suspensions.append(suspension)
        except Exception as e:
            logging.error(f"error while parsing suspension {row['readable_id']}: {e}")

    return suspensions
