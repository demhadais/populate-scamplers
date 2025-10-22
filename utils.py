import csv
from collections.abc import Iterable
import datetime
import logging
from pathlib import Path
from typing import Any
from uuid import UUID

from pydantic.main import BaseModel
from scamplepy import ScamplersClient
from scamplepy.query import LabQuery, PersonQuery


def to_snake_case(s: str):
    return s.lower().replace(" ", "_")


def str_to_float(s: str) -> float:
    f = float(s.replace(",", "").removesuffix("%"))

    if "%" in s:
        f = f / 100

    return f


def str_to_bool(s: str) -> bool | None:
    return {"TRUE": True, "FALSE": False}.get(s)


def shitty_date_str_to_eastcoast_9am(date_str: str) -> datetime.datetime:
    i_cant_believe_this_is_the_format_month, day, year = (
        date_str.split("-")[0].split("&")[0].split("/")
    )
    return datetime.datetime(
        year=int(year),
        month=int(i_cant_believe_this_is_the_format_month),
        day=int(day),
        hour=13,
        tzinfo=datetime.UTC,
    )


def date_str_to_eastcoast_9am(date_str: str) -> datetime.datetime:
    date = datetime.date.fromisoformat(date_str)
    return datetime.datetime(
        year=date.year, month=date.month, day=date.day, hour=13, tzinfo=datetime.UTC
    )


def _recursive_getattr(obj: Any, path: str) -> Any:
    split = path.split(".")
    if len(split) == 1:
        return getattr(obj, path)
    else:
        return _recursive_getattr(getattr(obj, split[0]), ".".join(split[1:]))


def property_id_map(
    property_path: str, id_path: str, data: list[Any]
) -> dict[str, UUID]:
    map = {
        _recursive_getattr(d, property_path): _recursive_getattr(d, id_path)
        for d in data
    }
    assert len(map) == len(data), f"property {property_path} is not unique"
    return map


def _rename_csv_fields(
    csv: Iterable[dict[str, Any]],
    field_renaming: dict[str, str],
) -> list[dict[str, Any]]:
    return [
        {
            field_renaming.get(field, to_snake_case(field)): value
            for field, value in row.items()
            if field is not None
        }
        for row in csv
    ]


class CsvSpec(BaseModel):
    path: Path | None = None
    head_row: int = 0
    onedrive_file_id: str | None = None
    field_renaming: dict[str, str] = {}


def read_csv(spec: CsvSpec) -> list[dict[str, Any]]:
    csv_path = spec.path
    if csv_path is None:
        raise NotImplementedError("fetching data from OneDrive is not yet supported")

    with csv_path.open(encoding="UTF-8-SIG") as f:
        for _ in range(0, spec.head_row):
            next(f)

        data = csv.DictReader(f, quoting=csv.QUOTE_NOTNULL)

        return _rename_csv_fields(data, spec.field_renaming)


def row_is_empty(
    row: dict[str, Any],
    required_keys: set[str],
    empty_equivalent: dict[str, list[Any]] = {},
) -> bool:
    is_empty1 = all(row[key] is None for key in required_keys)
    is_empty2 = any(row[key] in empty_equivalent[key] for key in empty_equivalent)

    if is_empty1 or is_empty2:
        return True

    is_partially_empty = any(row[key] is None for key in required_keys)

    if is_partially_empty:
        logging.warning(
            f"skipping partially empty row (required keys: {required_keys}):\n {row}"
        )
        return True

    return False


async def get_lab_name_id_map(client: ScamplersClient) -> dict[str, UUID]:
    labs = await client.list_labs(LabQuery(limit=9_999))
    labs = property_id_map("info.summary.name", "info.id_", labs)
    labs = labs | {name.lower(): id for name, id in labs.items()}

    return labs


async def get_person_email_id_map(client: ScamplersClient) -> dict[str, UUID]:
    people = await client.list_people(PersonQuery(limit=9_999))
    people = property_id_map("info.summary.email", "info.id_", people)
    people = people | {email.lower(): id for email, id in people.items()}

    return people
