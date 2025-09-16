from pathlib import Path

from pydantic_settings import (
    BaseSettings,
    CliApp,
    CliPositionalArg,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)
from scamplepy import ScamplersClient
from scamplepy.query import InstitutionQuery

from models.institutions import create_institutions, write_institutions_to_cache
from read_write import CsvSpec, read_csv

POPULATE_SCAMPLERS = "populate-scamplers"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix=POPULATE_SCAMPLERS.upper(),
        cli_kebab_case=True,
    )

    config_path: Path = Path.home() / ".config" / POPULATE_SCAMPLERS / "settings.toml"
    cache_dir: Path = Path.home() / ".cache" / POPULATE_SCAMPLERS
    api_base_url: str
    api_key: str
    accept_invalid_certificates: bool = False
    institutions: CsvSpec | None = None
    people: CsvSpec | None = None
    labs: CsvSpec | None = None
    specimens: CsvSpec | None = None
    suspensions: CsvSpec | None = None
    suspension_pools: CsvSpec | None = None
    gems: CsvSpec | None = None
    gems_suspensions: CsvSpec | None = None
    cdna: CsvSpec | None = None
    libraries: CsvSpec | None = None
    sequencing_submissions: CsvSpec | None = None
    dataset_dirs: CliPositionalArg[list[Path]] = []
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

    async def cli_cmd(self):
        client = ScamplersClient(
            api_base_url=self.api_base_url,
            api_key=self.api_key,
            accept_invalid_certificates=self.accept_invalid_certificates,
        )

        if self.institutions is not None:
            data = read_csv(self.institutions)
            created_institutions = await create_institutions(
                client=client,
                data=data,
                cache_dir=self.cache_dir,
            )
            write_institutions_to_cache(
                cache_dir=self.cache_dir,
                institution_creation_results=created_institutions,
            )

        institutions = await client.list_institutions(InstitutionQuery())
        name_institution_map = {
            institution.name: institution for institution in institutions
        }
        assert len(institutions) == len(name_institution_map), (
            "institutions do not have unique names"
        )


async def update_scamplers_api():
    CliApp.run(Settings)
