"""Silver layer — 3NF model with enforced data quality.

Owner: Person 2.

Reads ``config.table("bronze", <name>)`` for every table in ``config.tables``.
Writes conformed tables plus a ``quarantine_<name>`` holding the rows that
failed a rule, with the reason attached.
"""

from __future__ import annotations

from typing import Any

from tpch_lakehouse.cli import run_layer
from tpch_lakehouse.config import Config

LAYER = "silver"


def run(spark: Any, config: Config) -> None:
    raise NotImplementedError("TODO(person-2): type, deduplicate, apply rules, quarantine")


def main() -> None:
    run_layer(__doc__, run)


if __name__ == "__main__":
    main()
