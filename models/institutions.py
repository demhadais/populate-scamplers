from collections.abc import Generator
from typing import Any

import httpx

from utils import row_is_empty


def _parse_row(row: dict[str, Any], empty_fn: str) -> dict[str, Any] | None:
    required_keys = {"id", "name"}

    if row_is_empty(row, required_keys, empty_fn):
        return None

    data = {key: row[key] for key in ["id", "name"]}

    # These are duplicates
    if data["name"] in (
        "Jackson Laboratory for Genomic Medicine",
        "Jackson Laboratory for Mammalian Genetics",
        "JAX Mice, Clinical, and Research Services",
        "University of Connecticut Storrs",
    ):
        return None

    return data


async def csv_to_new_institutions(
    client: httpx.AsyncClient,
    institutions_url: str,
    data: list[dict[str, Any]],
    empty_fn: str,
) -> Generator[dict[str, Any]]:
    pre_existing_institutions = {
        inst["id"] for inst in (await client.get(institutions_url)).json()
    }
    new_institutions = (_parse_row(row, empty_fn) for row in data)
    new_institutions = (
        inst
        for inst in new_institutions
        if not (inst is None or inst["id"] in pre_existing_institutions)
    )

    return new_institutions
