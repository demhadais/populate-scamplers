from pathlib import Path

from pydantic import BaseModel
from pydantic_settings import (
    BaseSettings,
    CliPositionalArg,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)
from scamplepy import ScamplersClient

from models.institutions import create_institutions, write_institutions_to_cache

POPULATE_SCAMPLERS = "populate-scamplers"


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
    )

    config_path: Path = Path.home() / ".config" / POPULATE_SCAMPLERS / "settings.toml"
    cache_dir: Path = Path.home() / ".cache" / POPULATE_SCAMPLERS
    api_base_url: str
    api_key: str
    accept_invalid_certificates: bool = False
    csv_dir: CliPositionalArg[Path]
    skip: list[str] = []
    dataset_dirs: CliPositionalArg[list[Path]] = []
    file_renaming: dict[str, str] | None = None
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


async def update_scamplers_api():
    config = Config()  # pyright: ignore[reportCallIssue]
    client = ScamplersClient(
        api_base_url=config.api_base_url,
        api_key=config.api_key,
        accept_invalid_certificates=config.accept_invalid_certificates,
    )

    created_institutions = await create_institutions(
        client=client,
        csv_dir=config.csv_dir,
        file_renaming=config.file_renaming,
        csv_field_renaming=config.csv_field_renamings.institutions,
        cache_dir=config.cache_dir,
    )
    write_institutions_to_cache(
        cache_dir=config.cache_dir, institution_creation_results=created_institutions
    )
