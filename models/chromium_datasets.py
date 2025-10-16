import asyncio
from collections.abc import Generator
from pathlib import Path
from uuid import UUID
from scamplepy import ScamplersClient
from scamplepy.create import (
    NewCellrangerCountDataset,
    NewCellrangerMultiDataset,
    NewCellrangerVdjDataset,
    NewCellrangerarcCountDataset,
    NewCellrangeratacCountDataset,
)
from scamplepy.query import ChromiumDatasetQuery, LibraryQuery

from utils import property_id_map


def _parse_dir(
    path: Path, libraries: dict[str, UUID]
) -> (
    NewCellrangerCountDataset
    | NewCellrangerMultiDataset
    | NewCellrangerVdjDataset
    | NewCellrangerarcCountDataset
    | NewCellrangeratacCountDataset
    | None
):
    library_readable_ids = path.name.split("-")[:-1]
    print(library_readable_ids)
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
        pre_existing_datasets = tg.create_task(
            client.list_chromium_datasets(ChromiumDatasetQuery(limit=99_99))
        )

    libraries, pre_existing_datasets = (
        libraries.result(),
        pre_existing_datasets.result(),
    )
    libraries = property_id_map("info.summary.readable_id", "info.id_", libraries)
    pre_existing_datasets = property_id_map(
        "summary.data_path", "summary.id", pre_existing_datasets
    )

    datasets = (_parse_dir(path, libraries) for path in dataset_dirs)
    datasets = (
        ds
        for ds in datasets
        if not (ds is None or ds.inner.data_path in pre_existing_datasets)
    )

    return datasets
