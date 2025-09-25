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
from scamplepy.errors import ScamplersErrorResponse

from models.institutions import (
    csv_to_new_institutions,
)
from models.labs import csv_to_new_labs
from models.people import csv_to_new_people
from models.specimen_measurements import csv_to_new_specimen_measurements
from models.specimens import csv_to_new_specimens
from utils import CsvSpec, read_csv

POPULATE_SCAMPLERS = "populate-scamplers"

_Req = TypeVar("_Req")
_Ret = TypeVar("_Ret")


async def _catch_exception(
    coro: Coroutine[_Ret, Any, _Ret], log_error: bool, error_path: Path | None
) -> _Ret | None:
    try:
        return await coro
    except ScamplersErrorResponse as e:
        if log_error:
            logging.error(e)
        if error_path is not None:
            infix = 0
            while error_path.exists():
                error_path = error_path.with_name(
                    error_path.stem + f"-{infix}" + error_path.suffix
                )
                if error_path.exists():
                    infix += 1

            error_path.write_bytes(e.error._0.to_json_bytes())


async def _send_requests(
    func: Callable[[_Req], Coroutine[_Ret, Any, _Ret]],
    data: Iterable[_Req],
    log_errors: bool,
    error_path_spec: tuple[Path, Callable[[_Req], str]] | None = None,
) -> list[tuple[_Req, _Ret]]:
    responses = []
    async with asyncio.TaskGroup() as tg:
        for d in data:
            coroutine = func(d)

            error_path = None
            if error_path_spec is not None:
                error_dir, error_path_creator = error_path_spec
                error_path = (error_dir / error_path_creator(d)).with_suffix(".json")

            coroutine_with_caught_exception = _catch_exception(
                coroutine, log_errors, error_path
            )
            task = tg.create_task(coroutine_with_caught_exception)
            responses.append((d, task))

    return [(d, r.result()) for d, r in responses]


async def _update_scamples_api(settings: "Settings"):
    client = ScamplersClient(
        api_base_url=settings.api_base_url,
        api_key=settings.api_key,
        accept_invalid_certificates=settings.accept_invalid_certificates,
    )

    errors_dir = settings.errors_dir
    log_errors = settings.log_errors

    if institutions := settings.institutions:
        data = read_csv(institutions)
        new_institutions = await csv_to_new_institutions(
            client,
            data,
        )
        error_path_spec = (errors_dir, lambda i: str(i.id)) if errors_dir else None
        await _send_requests(
            client.create_institution, new_institutions, log_errors, error_path_spec
        )

    if people := settings.people:
        data = read_csv(people)
        new_people = await csv_to_new_people(client, data)
        error_path_spec = (
            (errors_dir, lambda pers: pers.email.replace("@", "at"))
            if errors_dir
            else None
        )
        await _send_requests(
            client.create_person, new_people, log_errors, error_path_spec
        )

    if labs := settings.labs:
        data = read_csv(labs)
        new_labs = await csv_to_new_labs(
            client,
            data,
        )
        error_path_spec = (errors_dir, lambda lab: lab.name) if errors_dir else None
        await _send_requests(client.create_lab, new_labs, log_errors, error_path_spec)

    if specimens := settings.specimens:
        data = read_csv(specimens)
        new_specimens = await csv_to_new_specimens(client, data)
        error_path_spec = (
            (errors_dir, lambda spec: spec.inner.readable_id) if errors_dir else None
        )
        await _send_requests(
            client.create_specimen, new_specimens, log_errors, error_path_spec
        )

    if specimen_measurements := settings.specimen_measurements:
        data = read_csv(specimen_measurements)
        specimen_updates = await csv_to_new_specimen_measurements(client, data)
        error_path_spec = (
            (errors_dir, lambda upd: upd.specimen_id) if errors_dir else None
        )
        await _send_requests(
            client.update_specimen, specimen_updates, log_errors, error_path_spec
        )


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix=POPULATE_SCAMPLERS.upper(),
        cli_kebab_case=True,
    )

    config_path: Path = Path.home() / ".config" / POPULATE_SCAMPLERS / "settings.toml"
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
    log_errors: bool = True
    errors_dir: Path | None = None

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
