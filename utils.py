import csv
import datetime
import json
from collections.abc import Callable, Iterable
from pathlib import Path
from types import NoneType
from typing import Any

import httpx
from pydantic.dataclasses import dataclass
from pydantic.main import BaseModel

NO_LIMIT_QUERY = {"q": json.dumps({"limit": 999_999})}


def to_snake_case(s: str):
    return s.lower().replace(" ", "_")


def str_to_float(s: str) -> float:
    f = float(s.replace(",", "").removesuffix("%"))

    if "%" in s:
        f = f / 100

    return f


def str_to_int(s: str) -> int:
    return int(str_to_float(s))


def str_to_bool(s: str) -> bool | None:
    return {"TRUE": True, "FALSE": False}.get(s)


def _shitty_date_str_to_eastcoast_9am(date_str: str) -> datetime.datetime:
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
    try:
        date = datetime.date.fromisoformat(date_str)
    except ValueError:
        date = _shitty_date_str_to_eastcoast_9am(date_str)

    return datetime.datetime(
        year=date.year, month=date.month, day=date.day, hour=13, tzinfo=datetime.UTC
    )


def property_id_map(
    property_name: str, data: list[dict[str, Any]], id_path: str = "id"
) -> dict[str, str]:
    map = {d[property_name]: d[id_path] for d in data}
    assert len(map) == len(data), f"property {property_name} is not unique"

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
    id_key: str = "readable_id"
    empty_fn: str = "lambda row: False"


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
    id_key: str,
    empty_fn: str,
) -> bool:
    # Rows that are partially empty (meaning they lack some of the required keys) are allowed to pass through so that
    # the API catches them and tells us what's wrong
    is_empty1 = all(row[key] is None for key in required_keys)
    parsed_empty_fn: Callable[[dict[str, Any]], bool] = eval(empty_fn)
    is_empty2 = parsed_empty_fn(row)

    return is_empty1 or is_empty2


async def get_project_name_id_map(
    client: httpx.AsyncClient, labs_url: str
) -> dict[str, str]:
    labs = (await client.get(labs_url, params=NO_LIMIT_QUERY)).json()
    labs = property_id_map("name", labs)
    labs = labs | {name.lower(): id for name, id in labs.items()}

    return labs


async def get_person_email_id_map(
    client: httpx.AsyncClient, people_url: str
) -> dict[str, str]:
    people = (await client.get(people_url, params=NO_LIMIT_QUERY)).json()
    people = property_id_map("email", people)
    people = people | {email.lower(): id for email, id in people.items()}

    return people


def _strip(value: Any) -> Any:
    if isinstance(value, str):
        return value.strip()
    elif isinstance(value, list):
        return [_strip(inner) for inner in value]
    elif isinstance(value, dict):
        return {_strip(key): _strip(val) for key, val in value.items()}
    elif isinstance(value, datetime.datetime):
        return value.isoformat()
    elif isinstance(value, (int, float, bool, NoneType)):
        return value
    else:
        raise TypeError(f"{type(value)}")


def strip_str_values(data: dict[str, Any]) -> dict[str, Any]:
    new_dict = {}
    for key, val in data.items():
        if isinstance(val, dict):
            new_dict[key] = strip_str_values(val)
        else:
            new_dict[key] = _strip(val)

    return new_dict


def write_error(
    request: dict[str, Any],
    response: httpx.Response,
    error_dir: Path,
    filename_generator: Callable[[dict[str, Any]], str] = lambda d: d.get(
        "readable_id", d.get("name", "ERROR")
    ),
):
    error_subdir = error_dir / str(filename_generator(request))
    error_subdir.mkdir(parents=True, exist_ok=True)

    filename = len(list(error_subdir.iterdir()))
    error_path = error_subdir / Path(f"{filename}.json")

    try:
        response_body = response.json()
    except Exception:
        response_body = response.text

    to_write = {
        "request": request,
        "response": {
            "status": response.status_code,
            "extracted_body": response_body,
            "headers": {key: val for key, val in response.headers.items()},
        },
    }
    error_path.write_text(json.dumps(to_write))


@dataclass(kw_only=True, frozen=True)
class TenxAssaySpec:
    name: str
    sample_multiplexing: str
    chemistry_version: str
    chromium_chip: str
    library_types: tuple[str, ...]
