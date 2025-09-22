from typing import Any
from uuid import UUID

from read_write import row_is_empty


def _parse_suspension_pool_row(
    suspension_pool_row: dict[str, Any],
    suspensions: list[dict[str, Any]],
    people: dict[str, UUID],
):
    keys = {""}
    is_empty = row_is_empty(suspension_pool_row, keys)

    if is_empty:
        return None
