import asyncio
import csv
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Protocol, Self, TypeVar
import logging

from pydantic.main import BaseModel


def _rename_csv_fields(
    csv: Iterable[dict[str, Any]],
    field_renaming: dict[str, str],
) -> list[dict[str, Any]]:
    return [
        {
            field_renaming.get(field, field.lower()): value
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
