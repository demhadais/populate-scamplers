import asyncio
import json
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

import httpx
from pydantic_settings import (
    BaseSettings,
    CliPositionalArg,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)

from models.institutions import (
    csv_to_new_institutions,
)
from models.people import csv_to_new_people
from models.projects import csv_to_new_projects
from models.specimen_measurements import csv_to_new_specimen_measurements
from models.specimens import csv_to_new_specimens
from models.suspension_measurements import csv_to_suspension_measurements
from models.suspensions import csv_to_new_suspensions
from utils import CsvSpec, read_csv, strip_str_values

POPULATE_CELLNOOR = "populate-cellnoor"


async def _post_many(
    client: httpx.AsyncClient, url: list[str] | str, data: Iterable[dict[str, Any]]
) -> list[tuple[dict[str, Any], httpx.Response]]:
    stripped_string_data = (strip_str_values(d) for d in data)
    responses = []
    async with asyncio.TaskGroup() as tg:
        if isinstance(url, str):
            for request_body in stripped_string_data:
                task = tg.create_task(client.post(url, json=request_body))
                responses.append((request_body, task))
        elif isinstance(url, list):
            for u, request_body in zip(url, stripped_string_data, strict=True):
                task = tg.create_task(client.post(u, json=request_body))
                responses.append((request_body, task))

    return [(request_body, task.result()) for request_body, task in responses]


def _write_error(
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

    # Since this script, at times, doesn't check for duplication errors, and we don't want to end up with a million
    # errors every time we run, we just skip writing these
    if response.status_code == 401:
        return

    to_write = {
        "request": request,
        "response": {
            "status": response.status_code,
            "extracted_body": response_body,
            "headers": {key: val for key, val in response.headers.items()},
        },
    }
    error_path.write_text(json.dumps(to_write))


def _write_errors(
    request_response_pairs: list[tuple[dict[str, Any], httpx.Response]],
    error_dir: Path,
    filename_generator: Callable[[dict[str, Any]], str] = lambda d: d.get(
        "readable_id", d.get("name", "ERROR")
    ),
):
    for req, resp in (
        (req, resp) for req, resp in request_response_pairs if resp.is_error
    ):
        _write_error(
            request=req,
            response=resp,
            error_dir=error_dir,
            filename_generator=filename_generator,
        )


async def _update_cellnoor_api(settings: "Settings"):
    client = httpx.AsyncClient(
        headers={"Authorization": f"Bearer {settings.api_token}"}, http2=True
    )

    errors_dir = settings.errors_dir

    institution_url = f"{settings.api_base_url}/institutions"
    if institutions := settings.institutions:
        data = read_csv(institutions)
        new_institutions = await csv_to_new_institutions(
            client,
            institution_url,
            data,
            id_key=settings.institutions.id_key,
            empty_fn=settings.institutions.empty_fn,
        )

        responses = await _post_many(
            client,
            institution_url,
            new_institutions,
        )
        _write_errors(responses, errors_dir)

    people_url = f"{settings.api_base_url}/people"
    if people := settings.people:
        data = read_csv(people)
        new_people = await csv_to_new_people(
            client,
            institution_url=institution_url,
            people_url=people_url,
            data=data,
            id_key=settings.people.id_key,
            empty_fn=settings.people.empty_fn,
        )
        responses = await _post_many(client, people_url, new_people)
        _write_errors(
            responses,
            errors_dir,
            lambda pers: (
                pers["email"].replace("@", "at")
                if pers["email"] is not None
                else "unknown-email"
            ),
        )

    project_url = f"{settings.api_base_url}/projects"
    if labs := settings.projects:
        data = read_csv(labs)
        new_projects = await csv_to_new_projects(
            client,
            project_url=project_url,
            data=data,
            id_key=settings.projects.id_key,
            empty_fn=settings.projects.empty_fn,
        )
        responses = await _post_many(client, project_url, new_projects)
        _write_errors(
            responses, errors_dir, lambda project: project["name"].replace(" ", "")
        )

    specimen_url = f"{settings.api_base_url}/specimens"
    if specimens := settings.specimens:
        data = read_csv(specimens)
        new_specimens = await csv_to_new_specimens(
            client,
            people_url=people_url,
            project_url=project_url,
            specimen_url=specimen_url,
            data=data,
            id_key=settings.specimens.id_key,
            empty_fn=settings.specimens.empty_fn,
        )
        request_response_pairs = await _post_many(client, specimen_url, new_specimens)

        _write_errors(request_response_pairs, errors_dir)

    def specimen_measurement_url_creator(specimen_id: str):
        return f"{specimen_url}/{specimen_id}/measurements"

    if specimen_measurements := settings.specimen_measurements:
        data = read_csv(specimen_measurements)
        new_specimen_measurements = await csv_to_new_specimen_measurements(
            client,
            specimen_url=specimen_url,
            people_url=people_url,
            specimen_measurement_url_creator=specimen_measurement_url_creator,
            data=data,
            id_key=settings.specimen_measurements.id_key,
            empty_fn=settings.specimen_measurements.empty_fn,
        )
        new_specimen_measurements = list(new_specimen_measurements)

        urls = [
            f"{specimen_url}/{specimen_id}/measurements"
            for specimen_id, _ in new_specimen_measurements
        ]
        data = [measurement for _, measurement in new_specimen_measurements]

        request_response_pairs = await _post_many(client, urls, data)
        _write_errors(
            request_response_pairs,
            errors_dir,
            filename_generator=lambda _: "specimen-measurements",
        )

    suspensions_url = f"{settings.api_base_url}/suspensions"
    multiplexing_tags_url = f"{settings.api_base_url}/multiplexing-tags"
    if suspensions := settings.suspensions:
        data = read_csv(suspensions)
        new_suspensions = await csv_to_new_suspensions(
            client,
            people_url=people_url,
            specimens_url=specimen_url,
            multiplexing_tags_url=multiplexing_tags_url,
            suspensions_url=suspensions_url,
            data=data,
            id_key=suspensions.id_key,
            empty_fn=suspensions.empty_fn,
        )
        request_response_pairs = await _post_many(
            client, f"{suspensions_url}", new_suspensions
        )
        _write_errors(request_response_pairs, errors_dir)

        new_suspension_measurements = await csv_to_suspension_measurements(
            people_url=people_url,
            suspensions_url=suspensions_url,
            data=data,
            client=client,
        )

        urls = [
            f"{suspensions_url}/{suspension_id}/measurements"
            for suspension_id, measurement_set in new_suspension_measurements
            for _ in measurement_set
        ]
        data = [
            measurement
            for _, measurement_set in new_suspension_measurements
            for measurement in measurement_set
        ]

        request_response_pairs = await _post_many(client, urls, data)
        _write_errors(
            request_response_pairs,
            errors_dir,
            filename_generator=lambda _: "suspension-measurements",
        )

    # if settings.suspension_pools and (
    #     settings.suspensions is None
    #     or settings.gems is None
    #     or settings.gems_suspensions is None
    # ):
    #     raise ValueError("cannot specify suspension pools without suspensions")
    # elif (
    #     (suspension_pools := settings.suspension_pools)
    #     and (suspensions := settings.suspensions)
    #     and (gems := settings.gems)
    #     and (gems_loading := settings.gems_suspensions)
    # ):
    #     suspension_pool_csv, suspensions_csv, gems_csv, gems_loading_csv = (
    #         read_csv(spec)
    #         for spec in [suspension_pools, suspensions, gems, gems_loading]
    #     )
    #     new_suspension_pools = await csvs_to_new_suspension_pools(
    #         client,
    #         suspension_pool_csv,
    #         suspension_data=suspensions_csv,
    #         gems_data=gems_csv,
    #         gems_loading_data=gems_loading_csv,
    #     )
    #     error_path_spec = (
    #         (errors_dir, lambda pool: pool.readable_id) if errors_dir else None
    #     )
    #     await _post_many(
    #         client.create_suspension_pool,
    #         new_suspension_pools,
    #         log_errors,
    #         error_path_spec,
    #     )

    # if settings.gems is not None and settings.gems_suspensions is None:
    #     raise ValueError("cannot specify GEMs CSV without GEMs-suspensions")

    # if (gems := settings.gems) and (gems_suspensions := settings.gems_suspensions):
    #     gems = read_csv(gems)
    #     gems_suspensions = read_csv(gems_suspensions)
    #     new_chromium_runs = await csv_to_chromium_runs(client, gems, gems_suspensions)
    #     error_path_spec = (
    #         (errors_dir, lambda run: run.inner.readable_id) if errors_dir else None
    #     )
    #     await _post_many(
    #         client.create_chromium_run,
    #         new_chromium_runs,
    #         log_errors,
    #         error_path_spec,
    #     )

    # if cdna := settings.cdna:
    #     data = read_csv(cdna)
    #     new_cdna = await csv_to_new_cdna(client, data)

    #     def extract_cdna_group_readable_ids(cdna_group: NewCdnaGroup) -> str:
    #         match cdna_group:
    #             case NewCdnaGroup.Single(c):
    #                 return c.readable_id
    #             case NewCdnaGroup.Multiple(m) | NewCdnaGroup.OnChipMultiplexing(m):
    #                 return "-".join(c.readable_id for c in m)

    #     error_path_spec = (
    #         (errors_dir, extract_cdna_group_readable_ids) if errors_dir else None
    #     )
    #     await _post_many(
    #         client.create_cdna,
    #         new_cdna,
    #         log_errors,
    #         error_path_spec,
    #     )

    # if libraries := settings.libraries:
    #     data = read_csv(libraries)
    #     new_libraries = await csv_to_new_libraries(client, data)
    #     error_path_spec = (
    #         (errors_dir, lambda lib: lib.readable_id) if errors_dir else None
    #     )
    #     await _post_many(
    #         client.create_library,
    #         new_libraries,
    #         log_errors,
    #         error_path_spec,
    #     )

    # if sequencing_submissions := settings.sequencing_submissions:
    #     data = read_csv(sequencing_submissions)
    #     new_sequencing_runs = await csv_to_sequencing_runs(client, data)
    #     error_path_spec = (
    #         (
    #             errors_dir,
    #             lambda seq_run: "_".join(
    #                 ilab_id for ilab_id in seq_run.additional_data["ilab_request_ids"]
    #             ),
    #         )
    #         if errors_dir
    #         else None
    #     )
    #     await _post_many(
    #         client.create_sequencing_run,
    #         new_sequencing_runs,
    #         log_errors,
    #         error_path_spec,
    #     )

    # if dataset_dirs := settings.dataset_dirs:
    #     chromium_datasets = await parse_chromium_dataset_dirs(client, dataset_dirs)
    #     error_path_spec = (errors_dir, lambda ds: ds.data_path) if errors_dir else None
    #     await _post_many(
    #         client.create_chromium_dataset,
    #         chromium_datasets,
    #         log_errors,
    #         error_path_spec,
    #     )


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix=POPULATE_CELLNOOR.upper(),
        cli_kebab_case=True,
    )

    config_path: Path = Path.home() / ".config" / POPULATE_CELLNOOR / "settings.toml"
    api_base_url: str
    api_token: str
    accept_invalid_certificates: bool = False
    institutions: CsvSpec | None = None
    people: CsvSpec | None = None
    projects: CsvSpec | None = None
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
    errors_dir: Path = Path(".errors")

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
        await _update_cellnoor_api(self)
