"""Monitoring — the metrics Procurement watches over time, and the alert.

Owner: Person 1.

Reads silver, writes two history tables into gold plus an alert query:

* ``monitor_spend_concentration``   — share of spend held by the top 10 suppliers
* ``monitor_supply_cost_by_region`` — weighted average supply cost per unit

**Spend** here is what we pay a supplier, not what a customer pays us:
``ps_supplycost * l_quantity``. A line item resolves to a supply agreement by
*both* keys together (part and supplier), never by one of them.
"""

from __future__ import annotations

import logging
from typing import Any

from tpch_lakehouse.cli import run_layer
from tpch_lakehouse.config import Config

SOURCE_LAYER = "silver"
TARGET_LAYER = "gold"

#: Everything is reported per calendar month, taken from the order date.
PERIOD = "MONTH"

#: Alert fires when one supplier holds more than this share of a period's spend.
#: Chosen, not derived — the README defends the number.
SINGLE_SUPPLIER_SHARE_THRESHOLD = 0.05

log = logging.getLogger(__name__)


def _base(config: Config) -> str:
    """One row per line item, with its spend, its supplier and its region."""
    t = lambda name: config.table(SOURCE_LAYER, name)  # noqa: E731
    return f"""
        SELECT
            date_trunc('{PERIOD}', o.o_orderdate)   AS period,
            s.s_suppkey                             AS supplier_key,
            s.s_name                                AS supplier_name,
            r.r_name                                AS region,
            ps.ps_supplycost * l.l_quantity         AS spend,
            l.l_quantity                            AS quantity
        FROM {t("lineitem")} l
        -- two-column foreign key: a part is tied to a supplier by both keys
        JOIN {t("partsupp")} ps
          ON ps.ps_partkey = l.l_partkey AND ps.ps_suppkey = l.l_suppkey
        JOIN {t("orders")} o   ON o.o_orderkey   = l.l_orderkey
        JOIN {t("supplier")} s ON s.s_suppkey    = l.l_suppkey
        JOIN {t("nation")} n   ON n.n_nationkey  = s.s_nationkey
        JOIN {t("region")} r   ON r.r_regionkey  = n.n_regionkey
    """


def spend_concentration(spark: Any, config: Config) -> Any:
    """Per month: total spend, and how much of it the ten largest suppliers hold."""
    return spark.sql(f"""
        WITH base AS ({_base(config)}),
        per_supplier AS (
            SELECT period, supplier_key, sum(spend) AS spend
            FROM base GROUP BY period, supplier_key
        ),
        ranked AS (
            SELECT *, row_number() OVER (PARTITION BY period ORDER BY spend DESC) AS rank
            FROM per_supplier
        )
        SELECT
            period,
            count(*)                                            AS suppliers,
            sum(spend)                                          AS total_spend,
            sum(CASE WHEN rank <= 10 THEN spend ELSE 0 END)     AS top10_spend,
            sum(CASE WHEN rank <= 10 THEN spend ELSE 0 END)
                / sum(spend)                                    AS top10_share
        FROM ranked
        GROUP BY period
        ORDER BY period
    """)


def supply_cost_by_region(spark: Any, config: Config) -> Any:
    """Per month and region: what a unit actually cost us.

    Weighted on purpose. ``avg(ps_supplycost)`` would answer a different and
    far less useful question — the average of a catalogue, not of a spend —
    and averaging those averages across regions would mean nothing at all.
    """
    return spark.sql(f"""
        WITH base AS ({_base(config)})
        SELECT
            period,
            region,
            sum(spend)                  AS spend,
            sum(quantity)               AS quantity,
            sum(spend) / sum(quantity)  AS avg_supply_cost_per_unit
        FROM base
        GROUP BY period, region
        ORDER BY period, region
    """)


def supplier_share_alert(spark: Any, config: Config) -> Any:
    """Rows only when a single supplier crosses the threshold. Empty is good news."""
    return spark.sql(f"""
        WITH base AS ({_base(config)}),
        per_supplier AS (
            SELECT period, supplier_key, any_value(supplier_name) AS supplier_name,
                   sum(spend) AS spend
            FROM base GROUP BY period, supplier_key
        )
        SELECT * FROM (
            SELECT period, supplier_key, supplier_name, spend,
                   spend / sum(spend) OVER (PARTITION BY period) AS share
            FROM per_supplier
        )
        WHERE share > {SINGLE_SUPPLIER_SHARE_THRESHOLD}
        ORDER BY period, share DESC
    """)


def run(spark: Any, config: Config) -> None:
    """Rebuild both history tables, then evaluate the alert."""
    for name, df in (
        ("monitor_spend_concentration", spend_concentration(spark, config)),
        ("monitor_supply_cost_by_region", supply_cost_by_region(spark, config)),
    ):
        target = config.table(TARGET_LAYER, name)
        # Always a full recompute: appending would duplicate history on a rerun.
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
        log.info("%-30s -> %s", name, target)

    breaches = supplier_share_alert(spark, config).collect()
    if not breaches:
        log.info("alert: no supplier above %.0f%% of spend", SINGLE_SUPPLIER_SHARE_THRESHOLD * 100)
    for row in breaches:
        log.warning(
            "ALERT %s: %s holds %.1f%% of spend",
            row["period"], row["supplier_name"], row["share"] * 100,
        )


def main() -> None:
    run_layer(__doc__, run)


if __name__ == "__main__":
    main()
