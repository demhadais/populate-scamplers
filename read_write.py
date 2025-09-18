import asyncio
import csv
from collections.abc import Iterable
import datetime
from pathlib import Path
from typing import Any, Protocol, Self, TypeVar
import logging
from uuid import UUID

from pydantic.main import BaseModel


def to_snake_case(s: str):
    return s.lower().replace(" ", "_")


def eastcoast_9am_from_date_str(date_str: str) -> datetime.datetime:
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
        }
        for row in csv
    ]


class CsvSpec(BaseModel):
    path: Path | None = None
    onedrive_file_id: str | None = None
    field_renaming: dict[str, str] = {}


def read_csv(spec: CsvSpec) -> list[dict[str, Any]]:
    csv_path = spec.path
    if csv_path is None:
        raise NotImplementedError("fetching data from OneDrive is not yet supported")

    with csv_path.open(encoding="UTF-8-SIG") as f:
        data = csv.DictReader(f)

        return _rename_csv_fields(data, spec.field_renaming)


class _ScamplersModel(Protocol):
    def to_json_string(self) -> str: ...

    @classmethod
    def from_json_string(cls, json_str: str) -> Self: ...


T = TypeVar("T", bound=_ScamplersModel)


def read_from_cache(cache_dir: Path, subdir_name: str, model: type[T]) -> list[T]:
    subdir = cache_dir / subdir_name
    subdir.mkdir(parents=True, exist_ok=True)

    return [model.from_json_string(p.read_text()) for p in subdir.iterdir()]


_T1 = TypeVar("_T1")
_T2 = TypeVar("_T2")


def partition_results(
    results: list[tuple[_T1, asyncio.Task[_T2]]],
) -> list[tuple[_T1, _T2]]:
    successes = []

    for item, task in results:
        exception = task.exception()
        if exception is None:
            successes.append((item, task.result()))
        else:
            logging.error(exception)

    return successes


def write_to_cache(
    cache_dir: Path, subdir_name: str, filename: str, data: _ScamplersModel
):
    subdir = cache_dir / subdir_name
    subdir.mkdir(parents=True, exist_ok=True)

    path = subdir / filename

    if path.exists():
        raise FileExistsError(f"cannot overwrite cached data at {path}")

    path.write_text(data.to_json_string())
