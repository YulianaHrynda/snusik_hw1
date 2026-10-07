# Databricks notebook source
# MAGIC %md
# MAGIC # Procurement - Questions 1 and 2
# MAGIC
# MAGIC Run after the gold task. This notebook only reads the gold tables.
# MAGIC Names come from the shared config; pass workspace overrides to `load_config` below.

# COMMAND ----------

import pathlib
import sys

REPO = next(
    p for p in [pathlib.Path.cwd(), *pathlib.Path.cwd().parents] if (p / "pyproject.toml").exists()
)
sys.path.insert(0, str(REPO / "src"))

import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

from tpch_lakehouse.config import load_config

config = load_config()
print(config.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1. Which ten suppliers hold the greatest inventory value?
# MAGIC
# MAGIC Inventory value is available quantity times supply cost, summed per supplier.
# MAGIC The denominator includes every supplier. This is an inventory snapshot,
# MAGIC independent of how often we ordered from a supplier.

# COMMAND ----------

inventory = config.table("gold", "supplier_inventory")
top10 = spark.sql(f"""
    SELECT rank, supplier_key, supplier_name, nation, region,
           inventory_value, total_inventory_value, inventory_share
    FROM {inventory} WHERE rank <= 10 ORDER BY rank
""")
display(top10)

top_rows = top10.collect()  # At most ten rows; keep the full inventory on Spark.
top_value = sum(row["inventory_value"] for row in top_rows)
total_value = top_rows[0]["total_inventory_value"] if top_rows else 0
top_share = float(top_value / total_value) if total_value else None
print(f"Top-ten inventory value: {top_value:,.2f}")
print(f"Total inventory value:   {total_value:,.2f}")
print(f"Top-ten share: {top_share:.2%}" if top_share is not None else "Top-ten share: undefined")

# COMMAND ----------

if top_rows:
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.barh(
        [row["supplier_name"] for row in top_rows],
        [float(row["inventory_value"]) / 1_000_000 for row in top_rows],
        color="#1f6f78",
    )
    ax.invert_yaxis()
    ax.set_xlabel("Inventory value (million currency units)")
    share_label = f" - {top_share:.2%} of total" if top_share is not None else ""
    ax.set_title(f"Ten suppliers with the greatest inventory value{share_label}")
    ax.grid(axis="x", alpha=0.2)
    fig.tight_layout()
    display(fig)
    plt.close(fig)
else:
    print("No suppliers available for the inventory chart.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2. Brand#32: price variation and use of the cheapest suppliers
# MAGIC
# MAGIC Each part is compared across every supplier offering it, including suppliers
# MAGIC with no orders. The percentage gap uses the cheapest cost as its denominator.
# MAGIC All suppliers tied at the minimum price count as cheapest. Parts with no orders
# MAGIC are shown separately; their cheapest-supplier quantity share is undefined.
# MAGIC All available order dates are included. Listed costs have no price history.

# COMMAND ----------

sourcing = config.table("gold", "brand32_sourcing")
comparison = spark.sql(f"""
    SELECT part_key, part_name, available_supplier_count, ordered_supplier_count,
           cheapest_supplier_keys, cheapest_cost, most_expensive_cost, cost_gap,
           100 * cost_gap_share AS cost_gap_pct,
           quantity, cheapest_quantity, other_quantity,
           100 * cheapest_quantity_share AS cheapest_quantity_pct,
           orders_from_cheapest, orders_only_from_cheapest, sourcing_status
    FROM {sourcing} ORDER BY cost_gap DESC, part_key
""")
display(comparison)

summary = spark.sql(f"""
    SELECT count(*) AS parts,
           sum(CASE WHEN line_count > 0 THEN 1 ELSE 0 END) AS ordered_parts,
           sum(CASE WHEN orders_from_cheapest THEN 1 ELSE 0 END) AS parts_using_cheapest,
           avg(cost_gap) AS average_cost_gap, max(cost_gap) AS largest_cost_gap,
           CASE WHEN sum(quantity) > 0
                THEN sum(cheapest_quantity) / sum(quantity) END AS cheapest_quantity_share
    FROM {sourcing}
""")
display(summary)

# COMMAND ----------

gap_rows = comparison.limit(15).collect()
if gap_rows:
    fig, ax = plt.subplots(figsize=(10, 6))
    positions = list(range(len(gap_rows)))
    for pos, row in enumerate(gap_rows):
        ax.plot(
            [float(row["cheapest_cost"]), float(row["most_expensive_cost"])],
            [pos, pos], color="#9ca3af", linewidth=3,
        )
    ax.scatter(
        [float(row["cheapest_cost"]) for row in gap_rows], positions,
        color="#1f6f78", s=60, label="Cheapest",
    )
    ax.scatter(
        [float(row["most_expensive_cost"]) for row in gap_rows], positions,
        color="#c45c36", s=25, label="Most expensive",
    )
    ax.set_yticks(positions, [f"Part {row['part_key']}" for row in gap_rows])
    ax.invert_yaxis()
    ax.set_xlabel("Listed supply cost per unit (currency units)")
    ax.set_title(f"Brand#32: the {len(gap_rows)} largest supplier price gaps")
    ax.legend()
    ax.grid(axis="x", alpha=0.2)
    fig.tight_layout()
    display(fig)
    plt.close(fig)
else:
    print("No Brand#32 offers available for the price-gap chart.")

# COMMAND ----------

status_rows = spark.sql(f"""
    SELECT sourcing_status, count(*) AS parts,
           sum(cheapest_quantity) AS cheapest_quantity, sum(other_quantity) AS other_quantity
    FROM {sourcing} GROUP BY sourcing_status
""").collect()  # At most four sourcing categories.
statuses = ["CHEAPEST_ONLY", "MIXED", "OTHER_ONLY", "NOT_ORDERED"]
labels = ["Cheapest only", "Mixed suppliers", "Other suppliers only", "Not ordered"]
counts = {row["sourcing_status"]: row["parts"] for row in status_rows}

if status_rows:
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    axes[0].barh(labels, [counts.get(status, 0) for status in statuses], color="#1f6f78")
    axes[0].invert_yaxis()
    axes[0].set_xlabel("Number of parts")
    axes[0].xaxis.set_major_locator(MaxNLocator(integer=True))
    axes[0].set_title("Brand#32 sourcing choices")
    cheapest_units = sum(float(row["cheapest_quantity"]) for row in status_rows)
    other_units = sum(float(row["other_quantity"]) for row in status_rows)
    total_units = cheapest_units + other_units
    axes[1].barh(["Ordered units"], [cheapest_units], color="#1f6f78", label="Cheapest")
    axes[1].barh(
        ["Ordered units"], [other_units], left=[cheapest_units],
        color="#c45c36", label="Other suppliers",
    )
    axes[1].set_xlabel("Quantity ordered")
    unit_label = f"{cheapest_units / total_units:.2%}" if total_units else "undefined"
    axes[1].set_title(f"Quantity sourced at the cheapest price: {unit_label}")
    axes[1].legend(loc="upper center", bbox_to_anchor=(0.5, -0.15), ncol=2)
    fig.tight_layout()
    display(fig)
    plt.close(fig)
else:
    print("No Brand#32 parts available for the sourcing chart.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interpretation
# MAGIC
# MAGIC Q1 describes supplier inventory concentration; spend concentration is a separate metric.
# MAGIC Q2 distinguishes using a cheapest supplier at least once from using cheapest suppliers
# MAGIC exclusively. The overall quantity share is calculated from summed quantities, rather
# MAGIC than averaging part-level percentages. Price gaps describe listed alternatives;
# MAGIC capacity, quality and delivery constraints are not represented in these calculations.
