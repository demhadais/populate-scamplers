import asyncio
import csv
from collections.abc import Mapping
from pathlib import Path
from typing import Literal

from pydantic import BaseModel
from pydantic_settings import (
    BaseSettings,
    CliPositionalArg,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)

POPULATE_SCAMPLERS = "populate-scamplers"
FILENAMES = Literal[
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


class ColumnRenaming(BaseModel):
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
    csv_dir: CliPositionalArg[Path]
    dataset_dirs: CliPositionalArg[list[Path]]
    csv_renaming: dict[str, str] | None = None
    column_renaming: ColumnRenaming | None = None
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


def file_matches_cache(cache_dir: Path, filename: FILENAMES, raw_contents: str) -> bool:
    return raw_contents == (cache_dir / filename).read_bytes()


def read_csv(
    csv_dir: Path,
    filename: FILENAMES,
    csv_renaming: Mapping[str, str],
) -> list[dict[str, str]]:
    csv_path = csv_dir / filename
    if renamed_path := csv_renaming.get(filename):
        csv_path = csv_path.with_name(renamed_path)

    contents = csv_path.read_text()

    return list(csv.DictReader(contents))


async def main() -> None:
    config = Config()


if __name__ == "__main__":
    asyncio.run(main())
