import asyncio
from collections.abc import Generator
from datetime import datetime
import logging
from typing import Any
from uuid import UUID
from scamplepy import ScamplersClient
from scamplepy.common import SuspensionMeasurementFields, VolumeUnit
from scamplepy.create import (
    NewOcmChromiumRun,
    NewOcmGems,
    NewSingleplexChipLoading,
    NewSingleplexChromiumRun,
    NewPoolMultiplexChromiumRun,
    NewSingleplexGems,
    NewPoolMultiplexGems,
)
from scamplepy.query import ChromiumRunQuery, SuspensionPoolQuery, SuspensionQuery

from utils import (
    date_str_to_eastcoast_9am,
    get_person_email_id_map,
    property_id_map,
    row_is_empty,
    str_to_bool,
    str_to_float,
)


def _parse_gems(
    loadings: list[dict[str, Any]],
    suspensions: dict[str, UUID],
    suspension_pools: dict[str, UUID],
    measured_at: datetime,
) -> NewSingleplexGems | NewPoolMultiplexGems | NewOcmGems | None:
    if any(
        loading["suspension_readable_id"] is not None
        and loading["suspension_pool_readable_id"] is not None
        for loading in loadings
    ):
        raise ValueError(
            "cannot specify both suspension_id and suspension_pool_readable_id for chip-loading"
        )

    n = len(loadings)
    if n > 1 and loadings[0]["suspension_pool_readable_id"]:
        raise ValueError("cannot load multiple suspension pools into a GEMs")

    gems_readable_id = loadings[0]["gems_readable_id"]
    parsed_loadings = []
    for loading in loadings:
        parsed_loading = {}
        try:
            if loading["suspension_readable_id"] is not None:
                parsed_loading["suspension_id"] = suspensions[
                    loading["suspension_readable_id"]
                ]
            elif loading["suspension_pool_readable_id"] is not None:
                parsed_loading["suspension_pool_id"] = suspension_pools[
                    loading["suspension_pool_readable_id"]
                ]
        except KeyError:
            susp = loading["suspension_readable_id"]
            if susp is None:
                susp = loading["suspension_pool_readable_id"]

            logging.error(f"suspension or suspension pool {susp} not found")

            return None

        parsed_loading["suspension_volume_loaded"] = SuspensionMeasurementFields.Volume(
            measured_at=measured_at,
            value=str_to_float(loading["suspension_volume_loaded_(µl)"]),
            unit=VolumeUnit.Microliter,
        )

        parsed_loading["buffer_volume_loaded"] = SuspensionMeasurementFields.Volume(
            measured_at=measured_at,
            value=str_to_float(loading["buffer_volume_loaded_(µl)"]),
            unit=VolumeUnit.Microliter,
        )

        if n == 1:
            parsed_loadings.append(parsed_loading)
        elif n > 1:
            parsed_loadings.append(NewSingleplexChipLoading(**parsed_loading))

    if n == 1 and loadings[0]["suspension_readable_id"] is not None:
        return NewSingleplexGems(readable_id=gems_readable_id, **parsed_loadings[0])

    if n == 1 and loadings[0]["suspension_pool_readable_id"] is not None:
        return NewPoolMultiplexGems(readable_id=gems_readable_id, **parsed_loadings[0])

    if n >= 1:
        return NewOcmGems(readable_id=gems_readable_id, loading=parsed_loadings)
    else:
        raise ValueError(f"unknown GEMs configuration: {loadings}")


def _gems_loading_succeeded(loadings: list[dict[str, Any]]):
    succeeded = True
    for loading in loadings:
        succeeded = succeeded and not str_to_bool(loading["clog/wetting_failure"])

    return succeeded


def _parse_chromium_run(
    chromium_run: list[dict[str, Any]],
    gems_loading: dict[str, list[dict[str, Any]]],
    people: dict[str, UUID],
    suspensions: dict[str, UUID],
    suspension_pools: dict[str, UUID],
    assays: dict[str, UUID],
) -> NewSingleplexChromiumRun | NewPoolMultiplexChromiumRun | NewOcmChromiumRun | None:
    data = {}

    data["readable_id"] = chromium_run[0]["chromium_run_readable_id"]
    data["run_at"] = measured_at = date_str_to_eastcoast_9am(
        chromium_run[0]["date_chip_run"]
    )
    data["run_by"] = people[chromium_run[0]["chip_run_by"]]
    data["assay_id"] = assays[chromium_run[0]["assay"]]
    data["succeeded"] = True

    gems = []
    for gems_row in chromium_run:
        try:
            loadings = gems_loading[gems_row["readable_id"]]
        except KeyError:
            logging.warning(
                f"GEMs {gems_row['readable_id']} does not have a complete loading specified"
            )
            continue

        data["succeeded"] = data["succeeded"] and _gems_loading_succeeded(loadings)

        gems.append(
            _parse_gems(
                loadings,
                suspensions=suspensions,
                suspension_pools=suspension_pools,
                measured_at=measured_at,
            )
        )
    gems = [g for g in gems if g is not None]

    if len({type(gem) for gem in gems}) != 1:
        logging.error(f"Chromium run with different GEMs types found: {chromium_run}")
        return None

    if isinstance(gems[0], NewSingleplexGems):
        return NewSingleplexChromiumRun(**data, gems=gems)

    if isinstance(gems[0], NewPoolMultiplexGems):
        return NewPoolMultiplexChromiumRun(**data, gems=gems)

    if isinstance(gems[0], NewOcmGems):
        return NewOcmChromiumRun(**data, gems=gems)

    raise ValueError(f"unknown configuration: {chromium_run}")


async def csv_to_chromium_runs(
    client: ScamplersClient,
    gems_data: list[dict[str, Any]],
    gems_loading_data: list[dict[str, Any]],
) -> Generator[
    NewSingleplexChromiumRun | NewPoolMultiplexChromiumRun | NewOcmChromiumRun
]:
    async with asyncio.TaskGroup() as tg:
        tasks = (
            get_person_email_id_map(client),
            client.list_suspensions(SuspensionQuery(limit=99_999)),
            client.list_suspension_pools(SuspensionPoolQuery(limit=99_999)),
            client.list_chromium_runs(ChromiumRunQuery()),
        )
        tasks = tuple(tg.create_task(task) for task in tasks)

    people, suspensions, suspension_pools, pre_existing_chromium_runs = tuple(
        task.result() for task in tasks
    )

    suspensions = property_id_map("info.summary.readable_id", "info.id_", suspensions)
    suspension_pools = property_id_map(
        "summary.readable_id", "summary.id", suspension_pools
    )
    pre_existing_chromium_runs = property_id_map(
        "info.summary.readable_id", "info.id_", pre_existing_chromium_runs
    )

    required_keys = {"chromium_run_readable_id", "assay"}
    chromium_runs = {
        gems_row["chromium_run_readable_id"]: []
        for gems_row in gems_data
        if not row_is_empty(gems_row, required_keys)
    }
    for gems_row in gems_data:
        if not row_is_empty(gems_row, required_keys):
            chromium_runs[gems_row["chromium_run_readable_id"]].append(gems_row)

    gems_loading = {
        gems_loading_row["gems_readable_id"]: []
        for gems_loading_row in gems_loading_data
    }
    for gems_loading_row in gems_loading_data:
        required_keys = {"suspension_volume_loaded_(µl)", "buffer_volume_loaded_(µl)"}
        if row_is_empty(gems_loading_row, required_keys):
            continue

        gems_loading[gems_loading_row["gems_readable_id"]].append(gems_loading_row)

    gems_loading = {key: lst for key, lst in gems_loading.items() if len(lst) > 0}

    # scamplepy doesn't yet have a client.list_tenx_assays method so we just hardcode these values for simplicity
    assays = {
        "Multiplex Flex Gene Expression v1 (GEM-X)": "01993aaa-318e-7768-a7ed-bc6ec6d6f0eb",
        "Multiplex Flex Gene Expression v1 (Next GEM)": "01993aaa-3196-7c29-8bfb-11dba37c66f2",
        "Single Cell 3' + Cell Surface Protein OCM v4": "01993aaa-31a3-7ca5-9098-80f67a7f8406",
        "Single Cell 3' Gene Expression + Cell Surface Protein v4": "01993aaa-319f-7799-aa80-adf9c0804c79",
        "Single Cell 3' Gene Expression OCM v4": "01993aaa-31ab-73d9-a163-2ba45df2d95f",
        "Single Cell 3' Gene Expression v4": "01993aaa-31a7-76d7-94a1-dcc5c488cf68",
        "Single Cell 5' Gene Expression +  V(D)J OCM v3": "01993aaa-31bd-7bd2-bed3-e599c0b866b9",
        "Single Cell 5' Gene Expression +  V(D)J v3": "01993aaa-31c4-71f5-9f2c-cd88e4642522",
        "Single Cell 5' Gene Expression OCM v3": "01993aaa-31b1-763c-9e58-5e83c6a03e39",
        "Single Cell 5' Gene Expression v3": "01993aaa-31b4-7426-b0b2-4421e2828ba7",
        "Single Cell 5' Gene Expression + CRISPR Screening v3": "01993aaa-31ae-7451-ba30-07c3911db83d",
        "Single Cell ATAC v2": "01993aaa-3183-7cc2-9710-47cbae4ece53",
        "Single Cell Multiome ATAC + Gene Expression v1": "01993aaa-318a-7536-aaca-8ce12682b037",
        "Singleplex Flex Gene Expression v1 (GEM-X)": "01993aaa-3192-7174-b1fd-18e0d7bc7ff0",
        "Singleplex Flex Gene Expression v1 (Next GEM)": "01993aaa-319b-723d-ba96-fbc2877e62f4",
    }
    assert len(assays.values()) == len(set(assays.values()))

    assays = {key: UUID(val) for key, val in assays.items()}

    chromium_runs = (
        _parse_chromium_run(
            chromium_run, gems_loading, people, suspensions, suspension_pools, assays
        )
        for chromium_run in chromium_runs.values()
        if chromium_run
    )

    return (
        run
        for run in chromium_runs
        if (run is not None and run.inner.readable_id not in pre_existing_chromium_runs)
    )
