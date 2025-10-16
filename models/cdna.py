import asyncio
from typing import Any
from uuid import UUID
import itertools
from scamplepy import ScamplersClient
from scamplepy.common import (
    LibraryType,
    MassUnit,
    NucleicAcidConcentration,
    NucleicAcidMeasurementData,
    VolumeUnit,
)
from scamplepy.create import NewCdna, NewCdnaGroup, NewCdnaMeasurement
from scamplepy.query import CdnaQuery, ChromiumRunQuery

from utils import (
    date_str_to_eastcoast_9am,
    get_person_email_id_map,
    row_is_empty,
    str_to_bool,
    str_to_float,
    to_snake_case,
)


def _parse_row(
    row: dict[str, Any], gems: dict[str, UUID], people: dict[str, UUID]
) -> NewCdna | None:
    required_keys = {
        "library_type",
        "date_prepared",
        "preparer_email",
        "gems_readable_id",
        "volume_(µl)",
        "n_amplification_cycles",
    }

    if row_is_empty(row, required_keys):
        return None

    data = {"readable_id": row["readable_id"]}

    library_type = to_snake_case(row["library_type"])
    library_type = {
        "gene_expression_flex": "gene_expression",
        "vdj-t": "vdj",
        "vdj-b": "vdj",
    }.get(library_type, library_type)
    data["library_type"] = LibraryType(library_type)

    data["preparer_ids"] = preparer_ids = [
        people[row[key]]
        for key in ["preparer_email", "preparer_2"]
        if row[key] is not None
    ]

    gems_id = gems.get(row["gems_readable_id"])
    if gems_id is None:
        return None
    data["gems_id"] = gems_id

    data["n_amplification_cycles"] = int(str_to_float(row["n_amplification_cycles"]))
    data["volume_mcl"] = str_to_float(row["volume_(µl)"])
    data["prepared_at"] = prepared_at = date_str_to_eastcoast_9am(row["date_prepared"])
    data["measurements"] = []
    try:
        data["measurements"].append(
            NewCdnaMeasurement(
                measured_by=preparer_ids[0],
                data=NucleicAcidMeasurementData.Electrophoretic(
                    measured_at=prepared_at,
                    instrument_name="TapeStation",
                    sizing_range=tuple(
                        int(row[key])
                        for key in [
                            "tapestation_gate_range_minimum_(bp)",
                            "tapestation_gate_range_maximum_(bp)",
                        ]
                    ),  # pyright: ignore[reportArgumentType]
                    concentration=NucleicAcidConcentration(
                        value=str_to_float(row["tapestation_concentration_(pg/µl)"]),
                        unit=(MassUnit.Picogram, VolumeUnit.Microliter),
                    ),
                    mean_size_bp=None,
                ),
            )
        )
    except AttributeError:
        pass

    additional_data = {}
    for key in ["experiment_id", "failure_notes", "storage_location", "notes"]:
        if value := row[key]:
            additional_data[key] = value

    for key in ["is_preamplification_product", "fails_quality_control"]:
        additional_data[key] = str_to_bool(row[key])

    data["additional_data"] = additional_data

    return NewCdna(**data)


async def csv_to_new_cdna(
    client: ScamplersClient, data: list[dict[str, Any]]
) -> list[NewCdnaGroup]:
    async with asyncio.TaskGroup() as tg:
        people = tg.create_task(get_person_email_id_map(client))
        chromium_runs = tg.create_task(
            client.list_chromium_runs(ChromiumRunQuery(limit=99_999))
        )
        pre_existing_cdna = tg.create_task(client.list_cdna(CdnaQuery(limit=99_999)))

    people, chromium_runs, pre_existing_cdna = (
        people.result(),
        chromium_runs.result(),
        pre_existing_cdna.result(),
    )

    pre_existing_cdna = {c.summary.readable_id: c for c in pre_existing_cdna}

    gems = {
        g.readable_id: g.id for chromium_run in chromium_runs for g in chromium_run.gems
    }

    cdna = (_parse_row(row, gems, people) for row in data)
    cdna = (c for c in cdna if not (c is None or c.readable_id in pre_existing_cdna))
    cdna = sorted(cdna, key=lambda c: c.gems_id)
    cdna_groups = itertools.groupby(cdna, key=lambda c: c.gems_id)
    ret_val = []
    for _, group in cdna_groups:
        group = list(group)
        if len(group) == 0:
            raise ValueError("this is a bug")
        elif len(group) == 1:
            ret_val.append(NewCdnaGroup.Single(group[0]))
        else:
            library_types = {str(c.library_type) for c in group}
            if len(library_types) == 1:
                ret_val.append(NewCdnaGroup.OnChipMultiplexing(group))
            else:
                ret_val.append(NewCdnaGroup.Multiple(group))

    return ret_val
