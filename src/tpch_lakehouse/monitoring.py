"""Monitoring — the metrics Procurement watches over time, and the alert.

Owner: Person 1.

Spend concentration (share held by the top 10 suppliers) and average supply
cost by region, both written as history tables so a dashboard plots them rather
than recomputing on every open.
"""

from __future__ import annotations

from typing import Any

from tpch_lakehouse.cli import run_layer
from tpch_lakehouse.config import Config

LAYER = "gold"

#: Alert fires when one supplier holds more than this share of total spend.
#: Chosen, not derived — the README has to defend the number.
SINGLE_SUPPLIER_SHARE_THRESHOLD = 0.05


def run(spark: Any, config: Config) -> None:
    raise NotImplementedError(
        "TODO(person-1): top-10 spend share per period, weighted avg supply cost "
        "by region, and the single-supplier threshold alert"
    )


def main() -> None:
    run_layer(__doc__, run)


if __name__ == "__main__":
    main()
