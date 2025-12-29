import asyncio
from collections.abc import Generator
from typing import Any

import httpx

from utils import (
    date_str_to_eastcoast_9am,
    get_lab_name_id_map,
    get_person_email_id_map,
    row_is_empty,
    to_snake_case,
)


def _parse_row(
    row: dict[str, Any],
    labs: dict[str, str],
    people: dict[str, str],
    id_key: str,
    empty_fn: str,
) -> dict[str, Any] | None:
    required_keys = {
        "name",
        "date_received",
        "submitter_email",
        "lab_name",
        "species",
        "tissue",
    }

    if row_is_empty(row, required_keys, id_key=id_key, empty_fn=empty_fn):
        return None

    data = {
        simple_key: row[simple_key] for simple_key in ["name", "readable_id", "tissue"]
    }

    data["lab_id"] = labs.get(row["lab_name"])

    if submitter_email := row["submitter_email"]:
        data["submitted_by"] = people[submitter_email.lower()]

    try:
        data["received_at"] = date_str_to_eastcoast_9am(row["date_received"])
    except TypeError:
        pass

    data["returned_by"] = None

    if row["returned_by"] not in (None, "0"):
        data["returned_by"] = people[row["returner_email"]]

    if row["date_returned"]:
        data["returned_at"] = date_str_to_eastcoast_9am(row["date_returned"])

    if row["species"] == "Homo sapiens + Mus musculus (PDX)":
        data["species"] = "homo_sapiens"
        data["host_species"] = "mus_musculus"
    elif row["species"]:
        data["species"] = to_snake_case(row["species"])

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
            "CMC": "carboxymethyl_cellulose",
            "OCT": "optimal_cutting_temperature_compound",
        }.get(preliminary_em)
        if data["embedded_in"] is None:
            data["embedded_in"] = to_snake_case(preliminary_em)

    match (row["type"], row["preservation_method"]):
        case ("Block" | "Curl", preservation) if preservation != "Fresh":
            preservation_to_fixative_and_type = {
                "Formaldehyde-derivative fixed": (
                    "formaldehyde_derivative",
                    "fixed_block",
                ),
                "Formaldehyde-derivative fixed & frozen": (
                    "formaldehyde_derivative",
                    "frozen_block",
                ),
                "Frozen": (None, "frozen_block"),
            }
            data["fixative"], data["type"] = preservation_to_fixative_and_type[
                preservation
            ]
        case ("Tissue", "Cryopreserved"):
            data["type"] = "cryopreserved_tissue"
        case ("Tissue", "DSP-fixed" | "Scale DSP-Fixed"):
            data["type"] = "fixed_tissue"
            data["fixative"] = "dithiobis_succinimidylpropionate"
        case ("Tissue", "Frozen"):
            data["type"] = "frozen_tissue"
        case ("Cell Suspension" | "Nucleus Suspension", "Cryopreserved"):
            data["type"] = "cryopreserved_suspension"
        case ("Cell Suspension" | "Nucleus Suspension", None):
            data["type"] = "fixed_or_fresh_suspension"
        case (
            "Cell Suspension" | "Nucleus Suspension" | "Cell Pellet" | "Nucleus Pellet",
            "Frozen",
        ):
            data["type"] = "frozen_suspension"
        case ("Cell Suspension" | "Nucleus Suspension", preservation) if (
            preservation != "Frozen"
        ):
            fixatives = {
                "Formaldehyde-derivative fixed": "formaldehyde_derivative",
                "DSP-fixed": "dithiobis_succinimidylpropionate",
                "Scale DSP-Fixed": "dithiobis_succinimidylpropionate",
            }
            try:
                data["fixative"] = fixatives[preservation]
                data["type"] = "fixed_suspension"
            except KeyError:
                if preservation == "Fresh":
                    data["type"] = "fresh_suspension"

    return data


async def csv_to_new_specimens(
    client: httpx.AsyncClient,
    people_url: str,
    lab_url: str,
    specimen_url: str,
    data: list[dict[str, Any]],
    id_key: str,
    empty_fn: str,
) -> Generator[dict[str, Any]]:
    async with asyncio.TaskGroup() as tg:
        people = tg.create_task(get_person_email_id_map(client, people_url))
        labs = tg.create_task(get_lab_name_id_map(client, lab_url))

    people = people.result()
    labs = labs.result()

    new_specimens = (
        _parse_row(row, labs=labs, people=people, id_key=id_key, empty_fn=empty_fn)
        for row in data
    )
    pre_existing_specimens = {
        s["readable_id"]
        for s in (await client.get(specimen_url, params={"limit": 99_999})).json()
    }

    new_specimens = (
        spec
        for spec in new_specimens
        if not (spec is None or spec["readable_id"] in pre_existing_specimens)
    )

    return new_specimens
