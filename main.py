import asyncio
import csv
from pathlib import Path
from typing import Any, Protocol, Self, Type, TypeVar
from uuid import UUID
from scamplepy.create import NewInstitution
from scamplepy import ScamplersClient
from pydantic import BaseModel
from pydantic_settings import (
    BaseSettings,
    CliPositionalArg,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)

POPULATE_SCAMPLERS = "populate-scamplers"
FILENAMES = [
    "institutions",
    "people",
    "labs",
    "specimens",
    "suspensions",
    "suspension-pools",
    "gems",
    "gems-suspensions",
    "cdna",
    "libraries",
    "sequencing-submissions",
]


class CsvFieldRenaming(BaseModel):
    institutions: dict[str, str] | None = None
    people: dict[str, str] | None = None
    labs: dict[str, str] | None = None
    specimens: dict[str, str] | None = None
    suspensions: dict[str, str] | None = None
    suspension_pools: dict[str, str] | None = None
    gems: dict[str, str] | None = None
    gems_suspensions: dict[str, str] | None = None
    cdna: dict[str, str] | None = None
    libraries: dict[str, str] | None = None
    sequencing_submissions: dict[str, str] | None = None


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix=POPULATE_SCAMPLERS.upper(),
        cli_parse_args=True,
        cli_kebab_case=True,
        cli_enforce_required=True,
    )

    config_path: Path = Path.home() / ".config" / POPULATE_SCAMPLERS / "settings.toml"
    cache_dir: Path = Path.home() / ".cache" / POPULATE_SCAMPLERS
    api_base_url: str = ""
    api_key: str = ""
    accept_invalid_certificates: bool = False
    csv_dir: CliPositionalArg[Path]
    skip: list[str] = []
    dataset_dirs: CliPositionalArg[list[Path]] = []
    file_renaming: dict[str, str] = {}
    csv_field_renamings: CsvFieldRenaming = CsvFieldRenaming()
    dry_run: bool = False
    print_requests: bool = False
    save_requests: Path | None = None
    print_responses: bool = False
    save_responses: Path | None = None

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            TomlConfigSettingsSource(
                settings_cls,
                toml_file=cls.model_fields["config_path"].default,
            ),
        )


def read_csv(
    csv_dir: Path,
    filename: str,
    csv_renaming: dict[str, str],
) -> list[dict[str, Any]]:
    if renamed := csv_renaming.get(filename):
        filename = renamed

    csv_path = csv_dir / filename

    with csv_path.open() as f:
        return list(csv.DictReader(f))


def rename_csv_fields(
    csv: list[dict[str, Any]], field_renaming: dict[str, str]
) -> list[dict[str, Any]]:
    return [
        {
            field_renaming[field]: value
            for field, value in row.items()
            if field in field_renaming
        }
        for row in csv
    ]


class ScamplersModel(Protocol):
    def to_json_string(self) -> str: ...

    @classmethod
    def from_json_string(cls, json_str: str) -> Self: ...


T = TypeVar("T", bound=ScamplersModel)


def read_from_cache(cache_dir: Path, subdir_name: str, model: Type[T]) -> list[T]:
    return [
        model.from_json_string(p.read_text())
        for p in (cache_dir / subdir_name).iterdir()
    ]


def parse_institutions(
    csv: list[dict[str, Any]],
    field_renaming: dict[str, str] | None,
    already_inserted_institutions: list[NewInstitution],
) -> list[NewInstitution]:
    if field_renaming is not None:
        csv = rename_csv_fields(csv, field_renaming)

    new_institutions = [
        NewInstitution(id=UUID(row["id"]), name=row["name"]) for row in csv
    ]

    return [
        inst for inst in new_institutions if inst not in already_inserted_institutions
    ]


async def write_to_cache(cache_dir: Path, subdir_name: str, data: list[tuple[str, T]]):
    subdir = cache_dir / subdir_name
    subdir.mkdir(parents=True, exist_ok=True)

    for filename, datum in data:
        path = subdir / filename

        if path.exists():
            raise ValueError(f"cannot overwrite cached data at {path}")

        path.write_text(datum.to_json_string())


async def send_institutions(
    client: ScamplersClient, csvs: dict[str, list[dict[str, Any]]], config: Config
) -> list[tuple[NewInstitution, Any]]:
    institutions = parse_institutions(
        csvs["institutions"],
        field_renaming=config.csv_field_renamings.institutions,
        already_inserted_institutions=read_from_cache(
            config.cache_dir, "institutions", NewInstitution
        ),
    )

    created_institutions: list[tuple[NewInstitution, asyncio.Task[Any]]] = []

    async with asyncio.TaskGroup() as tg:
        for new in institutions:
            institution = tg.create_task(client.create_institution(new))
            created_institutions.append((new, institution))

    return [(new, task.result()) for new, task in created_institutions]


async def write_institutions_to_cache(
    cache_dir: Path, institution_creation_results: list[tuple[NewInstitution, Any]]
):
    await write_to_cache(
        cache_dir,
        "institutions",
        [
            (f"{returned.id}.json", sent)
            for sent, returned in institution_creation_results
        ],
    )


async def main() -> None:
    config = Config()  # pyright: ignore[reportCallIssue]

    client = ScamplersClient(
        api_base_url=config.api_base_url,
        api_key=config.api_key,
        accept_invalid_certificates=config.accept_invalid_certificates,
    )

    csvs = {
        filename: read_csv(config.csv_dir, filename, config.file_renaming)
        for filename in [f for f in FILENAMES if f not in config.skip]
    }

    created_institutions = await send_institutions(client, csvs, config)
    await write_institutions_to_cache(config.cache_dir, created_institutions)


if __name__ == "__main__":
    asyncio.run(main())
