"""Gold layer — marts that answer the Procurement questions.

Owner: Person 3.

Reads silver only. Builds supplier inventory, monthly supplier spend, activity
for every available part/supplier pair, and the Brand#32 sourcing comparison
(Person 3), then per-part sourcing risk and supplier complaints on top of the
same spend definition (Person 4).
"""

from __future__ import annotations

import logging
from typing import Any

from tpch_lakehouse.cli import run_layer
from tpch_lakehouse.config import Config

LAYER = "gold"
SOURCE_LAYER = "silver"

COMPLAINT_PATTERN = "%Customer%Complaints%"

log = logging.getLogger(__name__)


def _suppliers(config: Config) -> str:
    t = lambda name: config.table(SOURCE_LAYER, name)  # noqa: E731
    return f"""
        SELECT s.s_suppkey AS supplier_key, s.s_name AS supplier_name,
               s.s_comment AS supplier_comment,
               n.n_name AS nation, r.r_name AS region
        FROM {t("supplier")} s
        JOIN {t("nation")} n ON n.n_nationkey = s.s_nationkey
        JOIN {t("region")} r ON r.r_regionkey = n.n_regionkey
    """


def _order_lines(config: Config) -> str:
    """One row per order line; resolve the supply cost using both keys."""
    t = lambda name: config.table(SOURCE_LAYER, name)  # noqa: E731
    return f"""
        SELECT l.l_orderkey AS order_key, l.l_partkey AS part_key,
               l.l_suppkey AS supplier_key,
               date_trunc('MONTH', o.o_orderdate) AS period,
               l.l_quantity AS quantity,
               ps.ps_supplycost * l.l_quantity AS spend
        FROM {t("lineitem")} l
        JOIN {t("partsupp")} ps
          ON ps.ps_partkey = l.l_partkey AND ps.ps_suppkey = l.l_suppkey
        JOIN {t("orders")} o ON o.o_orderkey = l.l_orderkey
    """


def supplier_inventory(spark: Any, config: Config) -> Any:
    """Q1: all suppliers, ranked by snapshot inventory value, including zero stock."""
    return spark.sql(f"""
        WITH suppliers AS ({_suppliers(config)}),
        inventory AS (
            SELECT ps_suppkey AS supplier_key,
                   sum(ps_availqty * ps_supplycost) AS inventory_value
            FROM {config.table(SOURCE_LAYER, "partsupp")}
            GROUP BY ps_suppkey
        ),
        totals AS (
            SELECT s.*, coalesce(i.inventory_value, 0) AS inventory_value
            FROM suppliers s LEFT JOIN inventory i USING (supplier_key)
        ),
        ranked AS (
            SELECT *,
                   row_number() OVER (ORDER BY inventory_value DESC, supplier_key) AS rank,
                   sum(inventory_value) OVER () AS total_inventory_value
            FROM totals
        )
        SELECT *,
               CASE WHEN total_inventory_value > 0
                    THEN inventory_value / total_inventory_value END AS inventory_share
        FROM ranked
    """)


def supplier_spend_monthly(spark: Any, config: Config) -> Any:
    """One row per active supplier/month, using the README's procurement spend."""
    return spark.sql(f"""
        WITH lines AS ({_order_lines(config)}),
        suppliers AS ({_suppliers(config)}),
        activity AS (
            SELECT period, supplier_key, count(*) AS line_count,
                   count(DISTINCT order_key) AS order_count,
                   sum(quantity) AS quantity, sum(spend) AS spend
            FROM lines GROUP BY period, supplier_key
        )
        SELECT a.*, s.supplier_name, s.supplier_comment, s.nation, s.region
        FROM activity a JOIN suppliers s USING (supplier_key)
    """)


def _part_supplier_activity(config: Config) -> str:
    t = lambda name: config.table(SOURCE_LAYER, name)  # noqa: E731
    return f"""
        WITH lines AS ({_order_lines(config)}),
        suppliers AS ({_suppliers(config)}),
        activity AS (
            SELECT part_key, supplier_key, count(*) AS line_count,
                   count(DISTINCT order_key) AS order_count,
                   sum(quantity) AS quantity, sum(spend) AS spend
            FROM lines GROUP BY part_key, supplier_key
        )
        SELECT ps.ps_partkey AS part_key, ps.ps_suppkey AS supplier_key,
               p.p_name AS part_name, p.p_brand AS brand,
               s.supplier_name, s.nation, s.region,
               ps.ps_availqty AS available_quantity, ps.ps_supplycost AS supply_cost,
               coalesce(a.line_count, 0) AS line_count,
               coalesce(a.order_count, 0) AS order_count,
               coalesce(a.quantity, 0) AS quantity, coalesce(a.spend, 0) AS spend
        FROM {t("partsupp")} ps
        JOIN {t("part")} p ON p.p_partkey = ps.ps_partkey
        JOIN suppliers s ON s.supplier_key = ps.ps_suppkey
        LEFT JOIN activity a
          ON a.part_key = ps.ps_partkey AND a.supplier_key = ps.ps_suppkey
    """


def part_supplier_activity(spark: Any, config: Config) -> Any:
    """All brands, one row per available pair; unused alternatives have zero activity."""
    return spark.sql(_part_supplier_activity(config))


def brand32_sourcing(spark: Any, config: Config) -> Any:
    """Q2: per-part price spread and usage of all suppliers tied for cheapest."""
    return spark.sql(f"""
        WITH offers AS ({_part_supplier_activity(config)}),
        priced AS (
            SELECT *, min(supply_cost) OVER (PARTITION BY part_key) AS cheapest_cost,
                      max(supply_cost) OVER (PARTITION BY part_key) AS most_expensive_cost
            FROM offers WHERE brand = 'Brand#32'
        ),
        summary AS (
            SELECT part_key, part_name, brand, cheapest_cost, most_expensive_cost,
                   count(*) AS available_supplier_count,
                   sum(CASE WHEN line_count > 0 THEN 1 ELSE 0 END) AS ordered_supplier_count,
                   array_sort(collect_set(CASE WHEN supply_cost = cheapest_cost
                                              THEN supplier_key END)) AS cheapest_supplier_keys,
                   sum(line_count) AS line_count,
                   sum(quantity) AS quantity, sum(spend) AS spend,
                   sum(CASE WHEN supply_cost = cheapest_cost THEN line_count ELSE 0 END)
                       AS cheapest_line_count,
                   sum(CASE WHEN supply_cost = cheapest_cost THEN quantity ELSE 0 END)
                       AS cheapest_quantity
            FROM priced
            GROUP BY part_key, part_name, brand, cheapest_cost, most_expensive_cost
        )
        SELECT *, most_expensive_cost - cheapest_cost AS cost_gap,
               (most_expensive_cost - cheapest_cost) / cheapest_cost AS cost_gap_share,
               quantity - cheapest_quantity AS other_quantity,
               CASE WHEN quantity > 0 THEN cheapest_quantity / quantity END
                   AS cheapest_quantity_share,
               CASE WHEN line_count > 0 THEN cheapest_line_count > 0 END
                   AS orders_from_cheapest,
               CASE WHEN line_count > 0 THEN cheapest_line_count = line_count END
                   AS orders_only_from_cheapest,
               CASE WHEN line_count = 0 THEN 'NOT_ORDERED'
                    WHEN cheapest_line_count = line_count THEN 'CHEAPEST_ONLY'
                    WHEN cheapest_line_count = 0 THEN 'OTHER_ONLY'
                    ELSE 'MIXED' END AS sourcing_status
        FROM summary
    """)


def part_sourcing(spark: Any, config: Config) -> Any:
    """Q3: one row per part on offer; flags parts bought from one supplier despite alternatives.

    The sole supplier's attributes are filled only when exactly one supplier was
    ordered from, so they can be grouped on without guessing which one it was.
    """
    return spark.sql(f"""
        WITH offers AS ({_part_supplier_activity(config)}),
        per_part AS (
            SELECT part_key, part_name, brand,
                   count(*) AS available_supplier_count,
                   sum(CASE WHEN line_count > 0 THEN 1 ELSE 0 END) AS ordered_supplier_count,
                   sum(line_count) AS line_count,
                   sum(quantity) AS quantity, sum(spend) AS spend
            FROM offers
            GROUP BY part_key, part_name, brand
        )
        SELECT p.*,
               p.available_supplier_count - p.ordered_supplier_count AS unused_supplier_count,
               CASE WHEN p.ordered_supplier_count = 0 THEN 'NOT_ORDERED'
                    WHEN p.ordered_supplier_count = 1 AND p.available_supplier_count > 1
                        THEN 'SINGLE_WITH_ALTERNATIVES'
                    WHEN p.ordered_supplier_count = 1 THEN 'SINGLE_NO_ALTERNATIVE'
                    ELSE 'MULTI_SUPPLIER' END AS sourcing_status,
               p.ordered_supplier_count = 1 AND p.available_supplier_count > 1
                   AS is_single_sourced,
               o.supplier_key AS sole_supplier_key, o.supplier_name AS sole_supplier_name,
               o.nation AS sole_supplier_nation, o.region AS sole_supplier_region
        FROM per_part p
        LEFT JOIN offers o
          ON o.part_key = p.part_key AND o.line_count > 0 AND p.ordered_supplier_count = 1
    """)


def supplier_complaints(spark: Any, config: Config) -> Any:
    """Q4: every supplier, flagged by complaint text, with its share of all spend.

    A NULL comment counts as no complaint rather than vanishing from both
    groups, so complaint and non-complaint shares always add up to one.
    """
    return spark.sql(f"""
        WITH lines AS ({_order_lines(config)}),
        suppliers AS ({_suppliers(config)}),
        activity AS (
            SELECT supplier_key, count(*) AS line_count, sum(quantity) AS quantity,
                   sum(spend) AS spend
            FROM lines GROUP BY supplier_key
        ),
        flagged AS (
            SELECT s.*,
                   coalesce(s.supplier_comment LIKE '{COMPLAINT_PATTERN}', false)
                       AS has_complaint,
                   coalesce(a.line_count, 0) AS line_count,
                   coalesce(a.quantity, 0) AS quantity, coalesce(a.spend, 0) AS spend
            FROM suppliers s LEFT JOIN activity a USING (supplier_key)
        )
        SELECT *, sum(spend) OVER () AS total_spend,
               CASE WHEN sum(spend) OVER () > 0 THEN spend / sum(spend) OVER () END
                   AS spend_share
        FROM flagged
    """)


def run(spark: Any, config: Config) -> None:
    """Recompute the gold marts; reruns replace aggregates rather than append."""
    config.create_schemas(spark)
    for name, build in (
        ("supplier_inventory", supplier_inventory),
        ("supplier_spend_monthly", supplier_spend_monthly),
        ("part_supplier_activity", part_supplier_activity),
        ("brand32_sourcing", brand32_sourcing),
        ("part_sourcing", part_sourcing),
        ("supplier_complaints", supplier_complaints),
    ):
        target = config.table(LAYER, name)
        build(spark, config).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            target
        )
        log.info("%-25s -> %s", name, target)


def main() -> None:
    run_layer(__doc__, run)


if __name__ == "__main__":
    main()
