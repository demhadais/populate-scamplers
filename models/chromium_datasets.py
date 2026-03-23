import asyncio
import json
import re
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

    dataset_files = list(get_cellranger_output_files(path))
    file_uploads = {}
    for fileset in dataset_files:
        for filename, open_file in fileset.files:
            file_uploads[filename] = open_file

    response = await client.post(
        f"{chromium_datasets_url}/{created_dataset['id']}/files",
        data=file_uploads,
    )

    if not response.ok:
        await write_error(
            request={
                "action": "uploaded metrics file",
                "name": created_dataset["name"],
                "file_paths": list(file_uploads.keys()),
            },
            response=response,
            error_dir=error_dir,
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
