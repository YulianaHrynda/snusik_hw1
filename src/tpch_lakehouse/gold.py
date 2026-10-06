"""Gold layer — marts that answer the Procurement questions.

Owner: Person 3.

Reads silver only. Inventory value by supplier, supply-cost spread within a
brand, single-source parts, and complaint signals buried in free text must all
be answerable from the tables built here.
"""

from __future__ import annotations

from typing import Any

from tpch_lakehouse.cli import run_layer
from tpch_lakehouse.config import Config

LAYER = "gold"


def run(spark: Any, config: Config) -> None:
    raise NotImplementedError("TODO(person-3): build the procurement marts")


def main() -> None:
    run_layer(__doc__, run)


if __name__ == "__main__":
    main()
