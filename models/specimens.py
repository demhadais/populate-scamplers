from pathlib import Path
from typing import Any
from uuid import UUID

from scamplepy import ScamplersClient
from scamplepy.create import (
    BlockFixative,
    FixedBlockEmbeddingMatrix,
    Species,
    SpecimenMeasurementData,
    SuspensionFixative,
    TissueFixative,
    NewCryopreservedTissue,
    NewFixedBlock,
    NewFixedTissue,
    NewFrozenBlock,
    NewFrozenTissue,
    NewCryopreservedSuspension,
    NewFixedOrFreshSuspension,
    NewFrozenSuspension,
    NewSpecimenMeasurement,
)
from scamplepy.query import LabQuery, PersonQuery

from read_write import eastcoast_9am_from_date_str, property_id_map, to_snake_case

type NewSpecimen = (
    NewFixedBlock
    | NewFrozenBlock
    | NewCryopreservedTissue
    | NewFixedTissue
    | NewFrozenTissue
    | NewCryopreservedSuspension
    | NewFixedOrFreshSuspension
    | NewFrozenSuspension
)


def _parse_specimens(
    data: list[dict[str, Any]],
    labs: dict[str, UUID],
    people_by_name: dict[str, UUID],
    people_by_email: dict[str, UUID],
    already_inserted_specimens: list[NewSpecimen],
) -> list[NewSpecimen]:
    common_keys = {
        "readable_id",
        "name",
    }

    specimens = []
    errors = []

    for row in data:
        if not row["date_received"]:
            continue
        if not row["submitter_email"]:
            continue
        if not row["lab_name"]:
            continue

        simple_data = {k: v for k, v in row.items() if k in common_keys}

        simple_data["lab_id"] = labs[row["lab_name"]]
        simple_data["submitted_by"] = people_by_email[row["submitter_email"].lower()]
        simple_data["received_at"] = eastcoast_9am_from_date_str(row["date_received"])
        if row["returned_by"]:
            simple_data["returned_by"] = people_by_name[row["returned_by"]]
        if row["date_returned"]:
            simple_data["returned_at"] = eastcoast_9am_from_date_str(
                row["date_returned"]
            )

        if not row["species"]:
            continue

        if row["species"] == "Homo sapiens + Mus musculus (PDX)":
            simple_data["species"] = [Species.HomoSapiens, Species.MusMusculus]
        else:
            simple_data["species"] = [Species(to_snake_case(row["species"]))]

        simple_data["notes"] = "; ".join(
            s
            for s in [
                row["condition"],
                row["tissue"],
                row["storage_details"],
                row["notes"],
            ]
        )
        if "cryostor" in row["storage_details"].lower():
            simple_data["storage_buffer"] = "cryostor"

        match (row["type"], row["preservation_method"]):
            case ("Block", "Formaldehyde-derivative fixed"):
                specimen = NewFixedBlock(
                    **simple_data,
                    fixative=BlockFixative.FormaldehydeDerivative,
                    embedded_in=FixedBlockEmbeddingMatrix(
                        to_snake_case(row["embedding_matrix"])
                    ),
                )
            case (_, _):
                continue
            case ("Block", "Frozen"):
                specimen = NewFrozenBlock
            case ("Block", "Fresh"):
                specimen = NewFrozenBlock
            case ("Tissue", "Cryopreserved"):
                specimen = NewCryopreservedTissue
            case ("Tissue", "DSP-fixed" | "Scale DSP-Fixed"):
                specimen = NewFixedTissue(
                    fixative=TissueFixative.DithiobisSuccinimidylpropionate
                )
            case ("Tissue", "Frozen"):
                specimen = NewFrozenTissue
            case ("Cell Suspension", "Cryopreserved"):
                specimen = NewCryopreservedSuspension
            case ("Cell Suspension" | "Nucleus Suspension", f):
                if f == "Formaldehyde-derivative fixed":
                    fixative = SuspensionFixative.FormaldehydeDerivative
                elif f == "DSP-fixed" or f == "Scale DSP-fixed":
                    fixative = SuspensionFixative.DithiobisSuccinimidylpropionate
                specimen = NewFixedOrFreshSuspension
            case ("Cell Suspension" | "Nucleus Suspension", "Frozen"):
                specimen = NewFrozenSuspension
            case ("", ""):
                continue
            case (ty, preservation):
                # This error reads nicely because `preservation` is something like "Formaldehyde-derivative fixed"
                errors.append(
                    ValueError(f"unexpected specimen details: {preservation} {ty}")
                )
                continue

        if specimen in already_inserted_specimens:
            continue

        specimens.append(specimen)

    if errors:
        raise ValueError(
            f"errors encountered while parsing specimens:\n{'\n'.join((f'{id}: {e}' for id, e in errors))}"
        )

    return specimens


def _parse_measurements(
    data: list[dict[str, Any]], people_by_email: dict[str, UUID]
) -> dict[str, list[NewSpecimenMeasurement]]:
    measurements = {}

    for row in data:
        if not row["measured_by"]:
            continue

        specimen_readable_id = row["specimen_readable_id"]
        measured_by = people_by_email[row["measured_by"]]
        if row["date_measured"]:
            measured_at = eastcoast_9am_from_date_str(row["date_measured"])
        else:
            measured_at = eastcoast_9am_from_date_str("1999-12-31")

        this_row_measurements = []
        if row["rin"]:
            this_row_measurements.append(
                NewSpecimenMeasurement(
                    measured_by=measured_by,
                    data=SpecimenMeasurementData.Rin(
                        measured_at=measured_at,
                        instrument_name="unknown",
                        value=float(row["rin"]),
                    ),
                )
            )
        if row["dv200"]:
            this_row_measurements.append(
                NewSpecimenMeasurement(
                    measured_by=measured_by,
                    data=SpecimenMeasurementData.Dv200(
                        measured_at=measured_at,
                        instrument_name="unknown",
                        value=float(row["dv200"].removesuffix("%")) / 100,
                    ),
                )
            )

        if specimen_readable_id in measurements:
            measurements[specimen_readable_id] = (
                measurements[specimen_readable_id] + this_row_measurements
            )
        else:
            measurements[specimen_readable_id] = this_row_measurements

    return measurements


async def csvs_to_new_specimens(
    client: ScamplersClient,
    specimen_csv: list[dict[str, Any]],
    measurement_csv: list[dict[str, Any]],
    cache_dir: Path,
) -> list[NewSpecimen]:
    labs = await client.list_labs(LabQuery())
    labs = property_id_map("info.summary.name", "info.id_", labs)

    people = await client.list_people(PersonQuery())
    people_by_name = property_id_map(
        "info.summary.name",
        "info.id_",
        [
            person
            for person in people
            if person.info.summary.name
            in [
                "Emily Soja",
                "Jessica Grassmann",
                "Shruti Bhargava",
                "Allison McNeilly",
            ]
        ],
    )
    people_by_email = property_id_map("info.summary.email", "info.id_", people)
    people_by_email = people_by_email | {
        k.lower(): v for k, v in people_by_email.items()
    }

    specimens = _parse_specimens(
        specimen_csv,
        labs=labs,
        people_by_name=people_by_name,
        people_by_email=people_by_email,
        already_inserted_specimens=[],
    )

    measurements = _parse_measurements(measurement_csv, people_by_email=people_by_email)

    for specimen in specimens:
        if m := measurements.get(specimen.inner.readable_id):
            specimen.inner.measurements = m

    return specimens
