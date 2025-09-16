import csv
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Protocol, Self, TypeVar

from pydantic.main import BaseModel


def _rename_csv_fields(
    csv: Iterable[dict[str, Any]],
    field_renaming: dict[str, str],
) -> list[dict[str, Any]]:
    return [
        {
            field_renaming[field]: value
            for field, value in row.items()
            if field in field_renaming
        }
        for row in csv
    ]


class CsvSpec(BaseModel):
    path: Path | None = None
    onedrive_file_id: str | None = None
    field_renaming: dict[str, str] | None = None


def read_csv(spec: CsvSpec) -> list[dict[str, Any]]:
    csv_path = spec.path
    if csv_path is None:
        raise NotImplementedError("fetching data from OneDrive is not yet supported")

    with csv_path.open() as f:
        data = csv.DictReader(f)

        field_renaming = spec.field_renaming
        if field_renaming is None:
            return list(data)

        return _rename_csv_fields(data, field_renaming)


class _ScamplersModel(Protocol):
    def to_json_string(self) -> str: ...

    @classmethod
    def from_json_string(cls, json_str: str) -> Self: ...


T = TypeVar("T", bound=_ScamplersModel)


def read_from_cache(cache_dir: Path, subdir_name: str, model: type[T]) -> list[T]:
    return [
        model.from_json_string(p.read_text())
        for p in (cache_dir / subdir_name).iterdir()
    ]


def write_to_cache(cache_dir: Path, subdir_name: str, data: list[tuple[str, T]]):
    subdir = cache_dir / subdir_name
    subdir.mkdir(parents=True, exist_ok=True)

    for filename, datum in data:
        path = subdir / filename

        if path.exists():
            raise FileExistsError(f"cannot overwrite cached data at {path}")

        path.write_text(datum.to_json_string())
