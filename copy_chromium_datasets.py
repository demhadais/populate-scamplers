import shutil
import sys
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path

METRICS_SUMMARY_FILENAMES = {
    "cellranger": "metrics_summary.csv",
    "cellranger-arc": "summary.csv",
    "cellranger-atac": "summary.json",
    "cellranger-multi": "metrics_summary.csv",
    "cellranger-multi-hto": "metrics_summary.csv",
    "cellranger-multi-ocm": "metrics_summary.csv",
    "cellranger-multi-vdj": "metrics_summary.csv",
    "cellranger-vdj": "metrics_summary.csv",
}


def _get_cellranger_directory(dataset_directory: Path) -> Path:
    return next(
        subdir for subdir in dataset_directory.iterdir() if "cellranger" in subdir.name
    )


def get_cmdline_file(dataset_directory: Path) -> Path:
    cellranger_directory = _get_cellranger_directory(dataset_directory)
    return cellranger_directory / "_files" / "_cmdline"


def get_pipeline_metadata_file(dataset_directory: Path) -> Path:
    return dataset_directory / "pipeline-metadata.json"


@dataclass(frozen=True, kw_only=True)
class CellrangerOutputFiles:
    metrics_file: Path
    web_summary_file: Path


def _get_files_from_per_sample_outs(
    dataset_directory: Path,
) -> Generator[CellrangerOutputFiles] | None:
    cellranger_directory = _get_cellranger_directory(dataset_directory)
    per_sample_outs = cellranger_directory / "per_sample_outs"

    if not per_sample_outs.exists():
        return

    return (
        CellrangerOutputFiles(
            metrics_file=sample_dir
            / METRICS_SUMMARY_FILENAMES[cellranger_directory.name],
            web_summary_file=sample_dir / "web_summary.html",
        )
        for sample_dir in per_sample_outs.iterdir()
        if sample_dir.is_dir()
    )


def _get_files_from_cellranger_directory(
    dataset_directory: Path,
) -> CellrangerOutputFiles:
    cellranger_directory = _get_cellranger_directory(dataset_directory)
    return CellrangerOutputFiles(
        metrics_file=cellranger_directory
        / METRICS_SUMMARY_FILENAMES[cellranger_directory.name],
        web_summary_file=cellranger_directory / "web_summary.html",
    )


def get_cellranger_output_files(
    dataset_directory: Path,
) -> Generator[CellrangerOutputFiles]:
    if metrics_files := _get_files_from_per_sample_outs(dataset_directory):
        return metrics_files

    return (p for p in [_get_files_from_cellranger_directory(dataset_directory)])


def _destination_file_path(
    source_dataset_directory: Path, source_file: Path, destination_directory: Path
) -> Path:
    cellranger_directory = _get_cellranger_directory(source_dataset_directory)

    per_sample_outs = cellranger_directory / "per_sample_outs"
    if per_sample_outs.exists():
        return (
            destination_directory
            / "per_sample_outs"
            / source_file.parent.name
            / source_file.name
        )
    else:
        return destination_directory / cellranger_directory.name / source_file.name


def _copy_dataset_directory(source_dataset_directory: Path, destination: Path):
    source_cellranger_directory = _get_cellranger_directory(source_dataset_directory)

    destination_directory = (
        destination
        / source_cellranger_directory.parent.name
        / source_cellranger_directory.name
    )
    if destination_directory.exists():
        return

    destination_files_directory = destination_directory / "_files"
    destination_files_directory.mkdir(exist_ok=True, parents=True)
    shutil.copyfile(
        get_cmdline_file(source_dataset_directory),
        destination_files_directory / "_cmdline",
    )

    source_pipeline_metadata = get_pipeline_metadata_file(source_dataset_directory)
    shutil.copyfile(
        source_pipeline_metadata, destination_directory / source_pipeline_metadata.name
    )

    for output_file_set in get_cellranger_output_files(source_dataset_directory):
        metrics_file_destination = _destination_file_path(
            source_dataset_directory=source_dataset_directory,
            source_file=output_file_set.metrics_file,
            destination_directory=destination_directory,
        )
        metrics_file_destination.parent.mkdir(exist_ok=True, parents=True)
        shutil.copyfile(output_file_set.metrics_file, metrics_file_destination)

        web_summary_destination = _destination_file_path(
            source_dataset_directory=source_dataset_directory,
            source_file=output_file_set.web_summary_file,
            destination_directory=destination_directory,
        )
        web_summary_destination.parent.mkdir(exist_ok=True, parents=True)
        shutil.copyfile(output_file_set.web_summary_file, web_summary_destination)


def main():
    top_level_source_directories = Path("/sc/service/delivery").glob("*/*/25E*")

    destination = Path(sys.argv[1])

    for dataset_directory in top_level_source_directories:
        _copy_dataset_directory(
            source_dataset_directory=dataset_directory, destination=destination
        )


if __name__ == "__main__":
    main()
