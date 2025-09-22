import asyncio
from collections.abc import Callable, Coroutine, Iterable
import logging
from pathlib import Path
from typing import Any, TypeVar

from pydantic_settings import (
    BaseSettings,
    CliPositionalArg,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)
from scamplepy import ScamplersClient

from models.institutions import (
    csv_to_new_institutions,
    write_institutions_to_cache,
)
from models.labs import csv_to_new_labs, write_labs_to_cache
from models.people import csv_to_new_people, write_people_to_cache
from models.specimens import csvs_to_new_specimens, write_specimens_to_cache
from models.suspensions import csv_to_new_suspensions
from read_write import CsvSpec, read_csv

POPULATE_SCAMPLERS = "populate-scamplers"

_Req = TypeVar("_Req")
_Ret = TypeVar("_Ret")


async def _catch_exception(coro: Coroutine[_Ret, Any, _Ret]) -> _Ret | None:
    try:
        return await coro
    except Exception as e:
        logging.error(e)


async def _send_requests(
    func: Callable[[_Req], Coroutine[_Ret, Any, _Ret]], data: Iterable[_Req]
) -> list[tuple[_Req, _Ret]]:
    responses = []
    async with asyncio.TaskGroup() as tg:
        for d in data:
            responses.append((d, tg.create_task(_catch_exception(func(d)))))

    return [(d, r.result()) for d, r in responses]


async def _update_scamples_api(settings: "Settings"):
    client = ScamplersClient(
        api_base_url=settings.api_base_url,
        api_key=settings.api_key,
        accept_invalid_certificates=settings.accept_invalid_certificates,
    )

    cache_dir = settings.cache_dir

    if settings.institutions is not None:
        data = read_csv(settings.institutions)
        new_institutions = await csv_to_new_institutions(
            data=data,
            cache_dir=cache_dir,
        )
        created_institutions = await _send_requests(
            client.create_institution, new_institutions
        )

        write_institutions_to_cache(
            cache_dir=settings.cache_dir,
            request_response_pairs=created_institutions,
        )

    if settings.people is not None:
        data = read_csv(settings.people)
        new_people = await csv_to_new_people(
            client=client,
            data=data,
            cache_dir=settings.cache_dir,
        )
        created_people = await _send_requests(client.create_person, new_people)
        write_people_to_cache(
            cache_dir=cache_dir, request_response_pairs=created_people
        )

    if settings.labs is not None:
        data = read_csv(settings.labs)
        new_labs = await csv_to_new_labs(client, data, cache_dir)
        created_labs = await _send_requests(client.create_lab, new_labs)
        write_labs_to_cache(cache_dir=cache_dir, request_response_pairs=created_labs)

    if (settings.specimens is None) != (settings.specimen_measurements is None):
        raise ValueError(
            "must specify specimens and specimen measurements together or not at all"
        )

    if settings.specimens is not None and settings.specimen_measurements:
        specimen_csv = read_csv(settings.specimens)
        measurements_csv = read_csv(settings.specimen_measurements)
        new_specimens = await csvs_to_new_specimens(
            client,
            specimen_csv=specimen_csv,
            measurement_csv=measurements_csv,
            cache_dir=cache_dir,
        )
        created_specimens = await _send_requests(client.create_specimen, new_specimens)
        write_specimens_to_cache(
            cache_dir=cache_dir, request_response_pairs=created_specimens
        )

    if settings.suspensions is not None:
        suspension_csv = read_csv(settings.suspensions)
        new_suspensions = await csv_to_new_suspensions(
            client, suspension_csv, cache_dir=cache_dir
        )
        new_suspensions = new_suspensions  # A hack for ruff


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
    specimen_measurements: CsvSpec | None = None
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
        await _update_scamples_api(self)
