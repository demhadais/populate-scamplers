import asyncio
from datetime import datetime
import re
from collections.abc import Generator
from pathlib import Path
from typing import Any
from uuid import UUID
from scamplepy import ScamplersClient
from scamplepy.create import (
    NewCellrangerCountDataset,
    NewCellrangerMultiDataset,
    NewCellrangerVdjDataset,
    NewCellrangerarcCountDataset,
    NewCellrangeratacCountDataset,
)
from scamplepy.query import ChromiumDatasetQuery, LabQuery, LibraryQuery

from utils import property_id_map


def _parse_dir(
    path: Path, libraries: dict[str, UUID], labs: dict[str, UUID]
) -> (
    NewCellrangerCountDataset
    | NewCellrangerMultiDataset
    | NewCellrangerVdjDataset
    | NewCellrangerarcCountDataset
    | NewCellrangeratacCountDataset
    | None
):
    library_readable_ids = re.findall(r"25E\d+-L\d?", path.name)

    try:
        library_ids = [libraries[id] for id in library_readable_ids]
    except KeyError:
        return None

    cellranger_dirs = {
        "cellranger": NewCellrangerCountDataset,
        "cellranger-multi": NewCellrangerMultiDataset,
        "cellranger-vdj": NewCellrangerVdjDataset,
        "cellranger-multi-ocm": NewCellrangerMultiDataset,
        "cellranger-arc": NewCellrangerarcCountDataset,
        "cellranger-atac": NewCellrangeratacCountDataset,
    }

    data_path = None
    for cellranger_dir in cellranger_dirs:
        data_path = path / cellranger_dir
        if data_path.exists():
            break

    if data_path is None:
        raise ValueError(f"did not find cellranger directory for {path}")

    data: dict[str, Any] = {"name": data_path.name}
    data["lab_id"] = labs[data_path.parent.parent.parent.name]

    stat = data_path.stat()
    data["delivered_at"] = datetime.fromtimestamp(stat.st_mtime)
    data["library_ids"] = library_ids

    per_sample_outs = data_path / "per_sample_outs"
    if per_sample_outs.exists():
        data["web_summaries"] = [
            (p / "web_summary.html").read_text() for p in per_sample_outs.iterdir()
        ]
    else:
        data["web_summaries"] = [(data_path / "web_summary.html").read_text()]

    print(data)

    return None


async def parse_chromium_dataset_dirs(
    client: ScamplersClient, dataset_dirs: list[Path]
) -> Generator[
    NewCellrangerCountDataset
    | NewCellrangerMultiDataset
    | NewCellrangerVdjDataset
    | NewCellrangerarcCountDataset
    | NewCellrangeratacCountDataset
]:
    async with asyncio.TaskGroup() as tg:
        libraries = tg.create_task(client.list_libraries(LibraryQuery(limit=99_999)))
        labs = tg.create_task(client.list_labs(LabQuery(limit=99_999)))
        pre_existing_datasets = tg.create_task(
            client.list_chromium_datasets(ChromiumDatasetQuery(limit=99_99))
        )

    libraries, labs, pre_existing_datasets = (
        libraries.result(),
        labs.result(),
        pre_existing_datasets.result(),
    )
    libraries = property_id_map("info.summary.readable_id", "info.id_", libraries)
    labs = property_id_map("info.summary.delivery_dir", "info.id_", labs)
    pre_existing_datasets = property_id_map(
        "summary.data_path", "summary.id", pre_existing_datasets
    )

    datasets = (_parse_dir(path, libraries, labs) for path in dataset_dirs)
    datasets = (
        ds
        for ds in datasets
        if not (ds is None or ds.inner.data_path in pre_existing_datasets)
    )

    return datasets
