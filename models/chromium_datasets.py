import asyncio
import json
import re
from contextlib import ExitStack
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiohttp

from copy_chromium_datasets import (
    get_cellranger_output_files,
    get_cmdline_file,
    get_pipeline_metadata_file,
)
from utils import NO_LIMIT_QUERY, property_id_map, write_error


def _get_delivered_at(dataset_directory: Path) -> str:
    pipeline_metadata = get_pipeline_metadata_file(dataset_directory).read_bytes()
    pipeline_metadata = json.loads(pipeline_metadata)
    delivered_at = pipeline_metadata["metadata_generated_date"]
    delivered_at = datetime.fromisoformat(delivered_at).replace(tzinfo=UTC)

    return delivered_at.isoformat()


async def _post_dataset(
    client: aiohttp.ClientSession,
    chromium_datasets_url: str,
    path: Path,
    libraries: dict[str, str],
    error_dir: Path,
):
    library_readable_ids1 = re.findall(r"25E\d+-L\d+", path.name)
    library_readable_ids2 = re.findall(r"26CH\d+-L\d+", path.name)

    try:
        library_ids = [
            libraries[id] for id in library_readable_ids1 + library_readable_ids2
        ]
    except KeyError:
        return None

    data: dict[str, Any] = {"name": path.name}
    data["delivered_at"] = _get_delivered_at(path)
    data["library_ids"] = library_ids
    data["cmdline"] = " ".join(get_cmdline_file(path).read_text().split()[0:2])

    # This specific dataset was slightly weird
    if data["name"] == "25E50-L4_WIBJ2" or data["name"] == "25E50-L3_WIBJ2":
        data["cmdline"] = "cellranger multi"

    response = await client.post(chromium_datasets_url, json=data)
    if not response.ok:
        await write_error(request=data, response=response, error_dir=error_dir)
        return

    created_dataset = await response.json()

    dataset_fileset = get_cellranger_output_files(path)

    with ExitStack() as stack:
        open_files = [
            (filename, stack.enter_context(path.open("rb")))
            for filename, path in dataset_fileset.files
        ]

        # NEVER CHANGE THE FOLLOWING CODE
        file_uploads = aiohttp.FormData(quote_fields=False, default_to_multipart=True)
        for filename, open_file in open_files:
            file_uploads.add_field(filename, open_file, filename=filename)

        response = await client.post(
            f"{chromium_datasets_url}/{created_dataset['id']}/files",
            data=file_uploads,
        )

    if not response.ok:
        await write_error(
            request={
                "action": "uploaded files",
                "name": created_dataset["name"],
                "file_paths": [fname for fname, _ in dataset_fileset.files],
            },
            response=response,
            error_dir=error_dir,
        )


async def _upload_files_for_one_dataset(
    client: aiohttp.ClientSession,
    chromium_datasets_url: str,
    dataset_id: str,
    path: Path,
    error_dir: Path,
):
    dataset_fileset = get_cellranger_output_files(path)

    with ExitStack() as stack:
        open_files = [
            (filename, stack.enter_context(path.open("rb")))
            for filename, path in dataset_fileset.files
        ]

        # NEVER CHANGE THE FOLLOWING CODE
        file_uploads = aiohttp.FormData(quote_fields=False, default_to_multipart=True)
        for filename, open_file in open_files:
            file_uploads.add_field(filename, open_file, filename=filename)

        response = await client.post(
            f"{chromium_datasets_url}/{dataset_id}/files",
            data=file_uploads,
        )

    if not response.ok:
        await write_error(
            request={
                "action": "uploaded files",
                "name": dataset_id,
                "file_paths": [fname for fname, _ in dataset_fileset.files],
            },
            response=response,
            error_dir=error_dir,
        )


async def upload_dataset_files(
    client: aiohttp.ClientSession,
    chromium_datasets_url: str,
    dataset_dirs: list[Path],
    errors_dir: Path,
):
    response = await client.get(chromium_datasets_url, params=NO_LIMIT_QUERY)
    pre_existing_datasets = await response.json()

    for dataset in pre_existing_datasets:
        if dataset["links"]["files"]:
            continue

        dataset_dir = [d for d in dataset_dirs if d.name == dataset["name"]]
        if len(dataset_dir) != 1:
            raise ValueError(f"how? {dataset['name']}")

        await _upload_files_for_one_dataset(
            client,
            chromium_datasets_url=chromium_datasets_url,
            dataset_id=dataset["id"],
            path=dataset_dir[0],
            error_dir=errors_dir,
        )


async def post_chromium_datasets(
    client: aiohttp.ClientSession,
    chromium_datasets_url: str,
    libraries_url: str,
    dataset_dirs: list[Path],
    errors_dir: Path,
):
    async with asyncio.TaskGroup() as tg:
        libraries = tg.create_task(client.get(libraries_url, params=NO_LIMIT_QUERY))
        pre_existing_datasets = tg.create_task(
            client.get(chromium_datasets_url, params=NO_LIMIT_QUERY)
        )

    libraries = await libraries.result().json()
    libraries = property_id_map("readable_id", libraries)

    pre_existing_datasets = await pre_existing_datasets.result().json()
    pre_existing_datasets = property_id_map("name", pre_existing_datasets)

    # Let's do it the inefficient way! Woohoo!
    # for path in dataset_dirs:
    #     if path.name in pre_existing_datasets:
    #         continue
    #     await _post_dataset(client, chromium_datasets_url, path, libraries, errors_dir)

    # I hate, loathe, and detest this language. The following code, which is what async is meant to do, doesn't work.

    tasks = []
    async with asyncio.TaskGroup() as tg:
        for path in dataset_dirs:
            if path.name in pre_existing_datasets:
                continue

            task = tg.create_task(
                _post_dataset(
                    client, chromium_datasets_url, path, libraries, errors_dir
                )
            )
            tasks.append(task)

    for task in tasks:
        task.result()
