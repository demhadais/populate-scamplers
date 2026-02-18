import asyncio
import logging
from collections.abc import Generator
from copy import deepcopy
from typing import Any, Literal

import httpx

from utils import (
    NO_LIMIT_QUERY,
    TenxAssaySpec,
    date_str_to_eastcoast_9am,
    get_person_email_id_map,
    property_id_map,
    row_is_empty,
    str_to_bool,
    str_to_float,
)


def _parse_gem_pools(
    loadings: list[dict[str, Any]],
    suspensions: dict[str, str],
    suspension_pools: dict[str, str],
) -> dict[str, Any] | None:
    gems_readable_id = loadings[0]["gems_readable_id"]
    parsed_loadings = []

    for loading in loadings:
        parsed_loading = {}

        if (
            loading["suspension_readable_id"] is not None
            and loading["suspension_readable_id"] in suspensions
        ):
            parsed_loading["suspension_id"] = suspensions[
                loading["suspension_readable_id"]
            ]
        elif (
            loading["suspension_pool_readable_id"] is not None
            and loading["suspension_readable_id"] in suspensions
        ):
            parsed_loading["suspension_pool_id"] = suspension_pools[
                loading["suspension_pool_readable_id"]
            ]
        else:
            return None

        parsed_loading["suspension_volume_loaded"] = {
            "value": str_to_float(loading["suspension_volume_loaded_(µl)"]),
            "unit": "microliter",
        }

        parsed_loading["buffer_volume_loaded"] = {
            "value": str_to_float(loading["buffer_volume_loaded_(µl)"]),
            "unit": "microliter",
        }

        if str(loading["tag_id"]).lower().startswith("ob"):
            for barcode in loading["tag_id"].split("+"):
                this = deepcopy(parsed_loading)
                this["ocm_barcode_id"] = barcode.lower()
                if this not in parsed_loadings:
                    parsed_loadings.append(this)
        else:
            parsed_loadings.append(parsed_loading)

    n = len(loadings)

    if n == 1:
        parsed_loading = parsed_loadings[0]
        gem_pool = {"readable_id": gems_readable_id, "loading": parsed_loading}
        return gem_pool

    if n >= 1:
        gem_pool = {"readable_id": gems_readable_id, "loading": parsed_loadings}
        return gem_pool


def _gems_loading_succeeded(loadings: list[dict[str, Any]]):
    succeeded = True
    for loading in loadings:
        succeeded = succeeded and not str_to_bool(loading["clog/wetting_failure"])

    return succeeded


def _plexy(
    gem_pools: list[dict[str, Any]],
) -> Literal["singleplex", "pool_multiplex", "on_chip_multiplexing"] | None:
    loading = gem_pools[0]["loading"]
    if isinstance(loading, dict) and loading.get("suspension_pool_id"):
        return "pool_multiplex"

    if isinstance(loading, dict) and loading.get("suspension_id"):
        return "singleplex"

    if isinstance(loading, list) and loading[0].get("suspension_id"):
        return "on_chip_multiplexing"

    else:
        return None


def _parse_chromium_run(
    chromium_run: list[dict[str, Any]],
    gems_loading: dict[str, list[dict[str, Any]]],
    people: dict[str, str],
    suspensions: dict[str, str],
    suspension_pools: dict[str, str],
    assays: dict[str, str],
) -> dict[str, Any] | None:
    data = {}

    data["readable_id"] = chromium_run[0]["chromium_run_readable_id"]

    run_at = chromium_run[0]["date_chip_run"]
    if run_at := run_at:
        data["run_at"] = date_str_to_eastcoast_9am(run_at)

    data["run_by"] = people[chromium_run[0]["chip_run_by"]]
    data["assay_id"] = assays[chromium_run[0]["assay"]]
    data["succeeded"] = True

    gem_pools = []
    for gems_row in chromium_run:
        try:
            loadings = gems_loading[gems_row["readable_id"]]
        except KeyError:
            logging.warning(
                f"GEMs {gems_row['readable_id']} does not have a complete loading specified"
            )
            continue

        data["succeeded"] = data["succeeded"] and _gems_loading_succeeded(loadings)

        gem_pools.append(
            _parse_gem_pools(
                loadings,
                suspensions=suspensions,
                suspension_pools=suspension_pools,
            )
        )

    gem_pools = [g for g in gem_pools if g is not None]
    if not (gem_pools):
        return None

    data["plexy"] = _plexy(gem_pools)
    data["gem_pools"] = gem_pools

    return data


async def csv_to_chromium_runs(
    client: httpx.AsyncClient,
    people_url: str,
    suspensions_url: str,
    suspension_pools_url: str,
    chromium_runs_url: str,
    tenx_assays_url: str,
    gem_pools_data: list[dict[str, Any]],
    gem_pools_loading_data: list[dict[str, Any]],
    id_key_for_gem_pools_data: str,
    empty_fn_for_gem_pools_data: str,
    id_key_for_loading_data: str,
    empty_fn_for_loading_data: str,
    assay_name_to_spec: dict[str, TenxAssaySpec],
) -> Generator[dict[str, Any]]:
    async with asyncio.TaskGroup() as tg:
        tasks = (
            get_person_email_id_map(client, people_url),
            client.get(suspensions_url, params=NO_LIMIT_QUERY),
            client.get(suspension_pools_url, params=NO_LIMIT_QUERY),
            client.get(chromium_runs_url, params=NO_LIMIT_QUERY),
            client.get(tenx_assays_url, params=NO_LIMIT_QUERY),
        )
        tasks = tuple(tg.create_task(task) for task in tasks)

    people, suspensions, suspension_pools, pre_existing_chromium_runs, tenx_assays = (
        tuple(task.result() for task in tasks)
    )
    suspensions, suspension_pools, pre_existing_chromium_runs, tenx_assays = (
        r.json()  # pyright: ignore[reportAttributeAccessIssue]
        for r in (
            suspensions,
            suspension_pools,
            pre_existing_chromium_runs,
            tenx_assays,
        )
    )

    tenx_assays = {TenxAssaySpec(**a): a["id"] for a in tenx_assays}
    tenx_assays = {
        assay_name: tenx_assays[assay_spec]
        for assay_name, assay_spec in assay_name_to_spec.items()
    }

    suspensions = property_id_map("readable_id", suspensions)
    suspension_pools = property_id_map("readable_id", suspension_pools)
    pre_existing_chromium_runs = property_id_map(
        "readable_id", pre_existing_chromium_runs
    )

    required_keys = {"chromium_run_readable_id", "assay"}
    chromium_runs = {
        gems_row["chromium_run_readable_id"]: []
        for gems_row in gem_pools_data
        if not row_is_empty(
            gems_row,
            required_keys,
            id_key=id_key_for_gem_pools_data,
            empty_fn=empty_fn_for_gem_pools_data,
        )
    }
    for gems_row in gem_pools_data:
        if not row_is_empty(
            gems_row,
            required_keys,
            id_key=id_key_for_gem_pools_data,
            empty_fn=empty_fn_for_gem_pools_data,
        ):
            chromium_runs[gems_row["chromium_run_readable_id"]].append(gems_row)

    gems_loading = {
        gems_loading_row["gems_readable_id"]: []
        for gems_loading_row in gem_pools_loading_data
    }
    for gems_loading_row in gem_pools_loading_data:
        required_keys = {"suspension_volume_loaded_(µl)", "buffer_volume_loaded_(µl)"}
        if row_is_empty(
            gems_loading_row,
            required_keys,
            id_key=id_key_for_loading_data,
            empty_fn=empty_fn_for_loading_data,
        ):
            continue

        gems_loading[gems_loading_row["gems_readable_id"]].append(gems_loading_row)

    gems_loading = {key: lst for key, lst in gems_loading.items() if len(lst) > 0}

    chromium_runs = (
        _parse_chromium_run(
            chromium_run,
            gems_loading,
            people,  # pyright: ignore[reportArgumentType]
            suspensions,
            suspension_pools,
            tenx_assays,
        )
        for chromium_run in chromium_runs.values()
        if chromium_run
    )

    return (
        run
        for run in chromium_runs
        if (run is not None and run["readable_id"] not in pre_existing_chromium_runs)
    )
