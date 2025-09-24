import datetime
import logging
from pathlib import Path
from typing import Any
from uuid import UUID

from scamplepy import ScamplersClient
from scamplepy.create import (
    BlockFixative,
    FixedBlockEmbeddingMatrix,
    FrozenBlockEmbeddingMatrix,
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
from scamplepy.responses import Specimen

from read_write import (
    eastcoast_9am_from_date_str,
    property_id_map,
    read_from_cache,
    row_is_empty,
    str_to_float,
    to_snake_case,
    write_to_cache,
)


def _parse_measurement_row(
    row: dict[str, Any],
    specimen_received_at: datetime.datetime,
    people: dict[str, UUID],
) -> list[NewSpecimenMeasurement]:
    required_keys = {"specimen_readable_id", "measured_by"}

    is_empty = row_is_empty(row, required_keys)
    if is_empty:
        return []

    measurements = []

    if row["date_measured"]:
        specimen_received_at = eastcoast_9am_from_date_str(row["date_measured"])
    else:
        specimen_received_at = specimen_received_at

    measured_by = people[row["measured_by"]]
    instrument_name = row["instrument_name"]

    if row["rin"]:
        measurements.append(
            NewSpecimenMeasurement(
                measured_by=measured_by,
                data=SpecimenMeasurementData.Rin(
                    measured_at=specimen_received_at,
                    instrument_name=instrument_name,
                    value=str_to_float(row["rin"]),
                ),
            )
        )
    if row["dv200"]:
        measurements.append(
            NewSpecimenMeasurement(
                measured_by=measured_by,
                data=SpecimenMeasurementData.Dv200(
                    measured_at=specimen_received_at,
                    instrument_name=instrument_name,
                    value=str_to_float(row["rin"]),
                ),
            )
        )

    return measurements


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


def _parse_specimen_row(
    row: dict[str, Any],
    measurements: dict[str, list[dict[str, Any]]],
    labs: dict[str, UUID],
    people: dict[str, UUID],
) -> NewSpecimen | None:
    necessary_keys = {
        "name",
        "date_received",
        "submitter_email",
        "lab_name",
        "species",
        "tissue",
    }

    is_empty = row_is_empty(row, necessary_keys)

    if is_empty:
        return None

    data = {
        simple_key: row[simple_key].strip()
        for simple_key in ["name", "readable_id", "tissue"]
    }

    data["lab_id"] = labs[row["lab_name"]]

    data["submitted_by"] = people[row["submitter_email"].lower()]

    data["received_at"] = eastcoast_9am_from_date_str(row["date_received"])

    data["returned_by"] = None

    if row["returned_by"] not in (None, "0"):
        data["returned_by"] = people[row["returner_email"]]

    if row["date_returned"]:
        data["returned_at"] = eastcoast_9am_from_date_str(row["date_returned"])

    if row["species"] == "Homo sapiens + Mus musculus (PDX)":
        data["species"] = [Species.HomoSapiens, Species.MusMusculus]
    else:
        data["species"] = [Species(to_snake_case(row["species"]))]

    data["additional_data"] = {
        key: row[key]
        for key in [
            "condition",
            "notes",
        ]
        if row[key] is not None
    }
    if len(data["additional_data"]) == 0:
        del data["additional_data"]

    parsed_measurements = []
    if measurement_rows := measurements.get(data["readable_id"]):
        for measurement_row in measurement_rows:
            parsed_measurements += _parse_measurement_row(
                measurement_row, specimen_received_at=data["received_at"], people=people
            )

    preliminary_em = row["embedding_matrix"]
    if preliminary_em is not None:
        data["embedded_in"] = {
            "CMC": FrozenBlockEmbeddingMatrix.CarboxymethylCellulose,
            "OCT": FrozenBlockEmbeddingMatrix.OptimalCuttingTemperatureCompound,
        }.get(preliminary_em)
        if data["embedded_in"] is None:
            data["embedded_in"] = FixedBlockEmbeddingMatrix(
                to_snake_case(preliminary_em)
            )

    match (row["type"], row["preservation_method"]):
        case ("Block" | "Curl", preservation):
            preservation_to_fixative_and_klass = {
                "Formaldehyde-derivative fixed": (
                    BlockFixative.FormaldehydeDerivative,
                    NewFixedBlock,
                ),
                "Formaldehyde-derivative fixed & frozen": (
                    BlockFixative.FormaldehydeDerivative,
                    NewFrozenBlock,
                ),
                "Frozen": (None, NewFrozenBlock),
            }
            data["fixative"], klass = preservation_to_fixative_and_klass[preservation]

            return klass(**data)
        case ("Tissue", "Cryopreserved"):
            return NewCryopreservedTissue(**data)
        case ("Tissue", "DSP-fixed" | "Scale DSP-Fixed"):
            return NewFixedTissue(
                **data, fixative=TissueFixative.DithiobisSuccinimidylpropionate
            )
        case ("Tissue", "Frozen"):
            return NewFrozenTissue(**data)
        case ("Cell Suspension" | "Nucleus Suspension", "Cryopreserved"):
            return NewCryopreservedSuspension(**data)
        case ("Cell Suspension" | "Nucleus Suspension", None):
            return NewFixedOrFreshSuspension(**data)
        case ("Cell Suspension" | "Nucleus Suspension", "Frozen"):
            return NewFrozenSuspension(**data)
        case ("Cell Suspension" | "Nucleus Suspension", preservation):
            fixatives = {
                "Formaldehyde-derivative fixed": SuspensionFixative.FormaldehydeDerivative,
                "DSP-fixed": SuspensionFixative.DithiobisSuccinimidylpropionate,
                "Scale DSP-Fixed": SuspensionFixative.DithiobisSuccinimidylpropionate,
                "Fresh": None,
            }
            data["fixative"] = fixatives[preservation]

            return NewFixedOrFreshSuspension(**data)
        case (ty, preservation):
            raise ValueError(f"unexpected specimen details: {preservation} {ty}")


SPECIMEN_SUBDIRS = {
    NewCryopreservedSuspension: "specimens/cryopreserved-suspension",
    NewCryopreservedTissue: "specimens/cryopreserved-tissue",
    NewFixedBlock: "specimens/fixed-block",
    NewFixedOrFreshSuspension: "specimens/fixed-or-fresh-suspension",
    NewFixedTissue: "specimens/fixed-tissue",
    NewFrozenBlock: "specimens/frozen-block",
    NewFrozenTissue: "specimens/frozen-tissue",
    NewFrozenSuspension: "specimens/frozen-suspension",
}


async def csvs_to_new_specimens(
    client: ScamplersClient,
    specimen_csv: list[dict[str, Any]],
    measurement_csv: list[dict[str, Any]],
    cache_dir: Path,
) -> list[NewSpecimen]:
    labs = await client.list_labs(LabQuery())
    labs = property_id_map("info.summary.name", "info.id_", labs)

    people = await client.list_people(PersonQuery())
    people = property_id_map("info.summary.email", "info.id_", people)
    people = people | {k.lower(): v for k, v in people.items()}

    measurements = {}
    for row in measurement_csv:
        specimen_readable_id = row["specimen_readable_id"]
        if specimen_readable_id is None:
            continue

        if specimen_readable_id in measurements:
            measurements[specimen_readable_id].append(row)
        else:
            measurements[specimen_readable_id] = [row]

    cached_specimens = []
    for ty in [
        NewFixedBlock,
        NewFrozenBlock,
        NewCryopreservedTissue,
        NewFixedTissue,
        NewFrozenTissue,
        NewCryopreservedSuspension,
        NewFixedOrFreshSuspension,
        NewFrozenSuspension,
    ]:
        cached_specimens += read_from_cache(cache_dir, SPECIMEN_SUBDIRS[ty], ty)

    specimens = []
    for row in specimen_csv:
        try:
            if specimen := _parse_specimen_row(
                row, measurements=measurements, labs=labs, people=people
            ):
                if specimen not in cached_specimens:
                    specimens.append(specimen)
        except Exception as e:
            logging.error(f"error while parsing specimen {row['readable_id']}: {e}")

    assert len(specimens) == len({s.inner.readable_id for s in specimens}), (
        "specimen IDs are not unique"
    )

    return specimens


def write_specimens_to_cache(
    cache_dir: Path, request_response_pairs: list[tuple[NewSpecimen, Specimen]]
):
    for request, response in request_response_pairs:
        if not request or not response:
            continue

        write_to_cache(
            cache_dir,
            subdir_name=SPECIMEN_SUBDIRS[type(request)],
            filename=f"{response.info.id_}.json",
            data=request,
        )
