from typing import Any
from uuid import UUID

from scamplepy import ScamplersClient
from scamplepy.create import (
    BlockFixative,
    FixedBlockEmbeddingMatrix,
    FrozenBlockEmbeddingMatrix,
    Species,
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
)
from scamplepy.query import LabQuery, PersonQuery, SpecimenQuery

from utils import (
    eastcoast_9am_from_date_str,
    property_id_map,
    row_is_empty,
    to_snake_case,
)


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


def _parse_row(
    row: dict[str, Any],
    labs: dict[str, UUID],
    people: dict[str, UUID],
) -> NewSpecimen | None:
    required_keys = {
        "name",
        "date_received",
        "submitter_email",
        "lab_name",
        "species",
        "tissue",
    }

    if row_is_empty(row, required_keys):
        return None

    data = {
        simple_key: row[simple_key] for simple_key in ["name", "readable_id", "tissue"]
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
            "storage_buffer",
            "notes",
        ]
        if row[key] is not None
    }
    if len(data["additional_data"]) == 0:
        del data["additional_data"]

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


async def csv_to_new_specimens(
    client: ScamplersClient, data: list[dict[str, Any]]
) -> list[NewSpecimen]:
    labs = await client.list_labs(LabQuery())
    labs = property_id_map("info.summary.name", "info.id_", labs)

    people = await client.list_people(PersonQuery())
    people = property_id_map("info.summary.email", "info.id_", people)
    people = people | {k.lower(): v for k, v in people.items()}

    new_specimens = (_parse_row(row, labs=labs, people=people) for row in data)
    pre_existing_specimens = {
        s.info.summary.readable_id
        for s in await client.list_specimens(SpecimenQuery(limit=99_999))
    }

    new_specimens = [
        s
        for s in new_specimens
        if not (s is None or s.inner.readable_id in pre_existing_specimens)
    ]

    return new_specimens
