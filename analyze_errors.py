import json
import re
from pathlib import Path


def find_errors(
    pattern: str, has_experiment: bool = True, numerical_part_starts: int = 0
):
    if has_experiment:
        sort_key = sort_thing_with_experiment_id
    else:
        sort_key = sort_thing_without_experiment_id

    return sorted(
        (
            {
                "path": path.name,
                "error": json.loads((path / "0.json").read_bytes()),
            }
            for path in Path(".errors").glob(pattern)
        ),
        key=lambda e: sort_key(e["path"], numerical_part_starts),
    )


def sort_thing_with_experiment_id(readable_id: str, _):
    parts = readable_id.split("-")
    part1, part2 = parts[:2]

    year = part1[:2]
    experiment = part1[3:].replace("H", "").replace("E", "")
    experiment = int(experiment)

    number = int(re.sub(r"[A-Z,a-z]", "", part2))

    return (year, experiment, number)


def sort_thing_without_experiment_id(readable_id: str, numerical_part_starts: int):
    return int(readable_id[numerical_part_starts:])
