from collections.abc import Generator
from typing import Any
from scamplepy import ScamplersClient
from scamplepy.create import NewSpecimenMeasurement, SpecimenMeasurementData
from scamplepy.query import PersonQuery, SpecimenQuery
from scamplepy.responses import Specimen
from scamplepy.update import SpecimenUpdateCommon

from utils import (
    date_str_to_eastcoast_9am,
    property_id_map,
    row_is_empty,
    str_to_float,
)


def _parse_specimen_measurement_row(
    row: dict[str, Any], people: dict[str, Any], specimens: dict[str, Specimen]
) -> SpecimenUpdateCommon | None:
    required_keys = {"specimen_readable_id", "measured_by"}

    if row_is_empty(row, required_keys):
        return None

    measurements = []

    specimen = specimens[row["specimen_readable_id"]]

    measured_by = people[row["measured_by"]]
    measurement_data = {key: row[key] for key in ["instrument_name"]}
    measurement_data["measured_at"] = (
        date_str_to_eastcoast_9am(row["date_measured"])
        if row["date_measured"]
        else specimen.info.summary.received_at
    )

    already_existing_measurements = [
        (m.measured_by, m.data) for m in specimen.measurements
    ]
    if rin := row["rin"]:
        data = SpecimenMeasurementData.Rin(**measurement_data, value=str_to_float(rin))
        if (measured_by, data) not in already_existing_measurements:
            measurements.append(
                NewSpecimenMeasurement(measured_by=measured_by, data=data)
            )

    if dv200 := row["dv200"]:
        data = SpecimenMeasurementData.Dv200(
            **measurement_data, value=str_to_float(dv200)
        )
        if (measured_by, data) not in already_existing_measurements:
            measurements.append(
                NewSpecimenMeasurement(measured_by=measured_by, data=data)
            )

    if not measurements:
        return None

    return SpecimenUpdateCommon(id=specimen.info.id_, measurements=measurements)


async def csv_to_new_specimen_measurements(
    client: ScamplersClient, data: list[dict[str, Any]]
) -> Generator[SpecimenUpdateCommon]:
    # In theory, it's be nice to get only the specimens we need by feeding in the sreadable_id`s, but this isn't supported by scamplers at the time of writing
    specimens = await client.list_specimens(SpecimenQuery(limit=99_999))
    specimen_id_map = {spec.info.summary.readable_id: spec for spec in specimens}

    if len(specimen_id_map) != len(specimens):
        raise ValueError("specimen readable IDs are not unique")

    people = await client.list_people(PersonQuery(limit=9_999))
    people = property_id_map("info.summary.email", "info.id_", people)

    updated = (
        _parse_specimen_measurement_row(row, people=people, specimens=specimen_id_map)
        for row in data
    )
    return (upd for upd in updated if upd is not None)
