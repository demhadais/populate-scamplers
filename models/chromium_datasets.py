import asyncio
import re
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from copy_chromium_datasets import (
    get_cellranger_output_files,
    get_cmdline_file,
)
from utils import NO_LIMIT_QUERY, property_id_map, write_error

CONTENT_TYPES = {".csv": "text/csv", ".html": "text/html", ".json": "application/json"}


async def _post_dataset(
    client: httpx.AsyncClient,
    chromium_datasets_url: str,
    path: Path,
    libraries: dict[str, str],
    error_dir: Path,
):
    library_readable_ids = re.findall(r"25E\d+-L\d?", path.name)

    try:
        library_ids = [libraries[id] for id in library_readable_ids]
    except KeyError:
        return None

    data: dict[str, Any] = {"name": path.name}
    data["delivered_at"] = datetime.fromtimestamp(
        path.stat().st_birthtime, tz=UTC
    ).isoformat()
    data["library_ids"] = library_ids
    data["cmdline"] = " ".join(get_cmdline_file(path).read_text().split()[0:2])

    response = await client.post(chromium_datasets_url, json=data)
    if response.is_error:
        write_error(request=data, response=response, error_dir=error_dir)
        return

    created_dataset = response.json()

    dataset_files = list(get_cellranger_output_files(path))

    def to_file_upload(paths: Iterable[Path]) -> dict[str, tuple[str, bytes, str]]:
        return {
            f"file{i}": (
                f"{path.parent.name}/{path.name}",
                path.read_bytes(),
                CONTENT_TYPES[path.suffix],
            )
            for i, path in enumerate(paths)
        }

    files = (fileset.metrics_file for fileset in dataset_files)
    files = to_file_upload(files)
    response = await client.post(
        f"{chromium_datasets_url}/{created_dataset['id']}/metrics",
        files=files,
    )
    if response.is_error:
        write_error(
            request={"action": "uploaded metrics file"},
            response=response,
            error_dir=error_dir,
        )

    files = (fileset.web_summary_file for fileset in dataset_files)
    files = to_file_upload(files)
    response = await client.post(
        f"{chromium_datasets_url}/{created_dataset['id']}/web-summaries",
        files=files,
    )
    if response.is_error:
        write_error(
            request={"action": "uploaded web summary file"},
            response=response,
            error_dir=error_dir,
        )


async def post_chromium_datasets(
    client: httpx.AsyncClient,
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

    libraries = libraries.result().json()
    libraries = property_id_map("readable_id", libraries)

    pre_existing_datasets = pre_existing_datasets.result().json()
    pre_existing_datasets = property_id_map("name", pre_existing_datasets)

    # Let's do it the inefficient way! Woohoo!
    for path in dataset_dirs:
        if path.name in pre_existing_datasets:
            continue
        await _post_dataset(client, chromium_datasets_url, path, libraries, errors_dir)

    # I loathe this language. What the hell about this causes a bug?

    # tasks = []
    # async with asyncio.TaskGroup() as tg:
    #     for path in dataset_dirs:
    #         if path.name in pre_existing_datasets:
    #             continue

    #         task = tg.create_task(
    #             _post_dataset(
    #                 client, chromium_datasets_url, path, libraries, errors_dir
    #             )
    #         )
    #         tasks.append(task)

    # for task in tasks:
    #     task.result()
