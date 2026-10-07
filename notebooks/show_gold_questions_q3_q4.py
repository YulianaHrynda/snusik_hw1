# Databricks notebook source
# MAGIC %md
# MAGIC # Procurement - Questions 3 and 4
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
from matplotlib.ticker import PercentFormatter

from tpch_lakehouse.config import load_config
from tpch_lakehouse.gold import COMPLAINT_PATTERN

config = load_config()
print(config.describe())

sourcing = config.table("gold", "part_sourcing")
complaints = config.table("gold", "supplier_complaints")
monthly = config.table("gold", "supplier_spend_monthly")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3. Parts bought from one supplier although others offer them
# MAGIC
# MAGIC A part is **single-sourced** when every order line for it went to the same
# MAGIC supplier, while `partsupp` lists at least one other supplier for that part.
# MAGIC Parts that only one supplier offers are counted separately: they have no
# MAGIC alternative, so ordering from that supplier is not a choice. Parts never ordered
# MAGIC are not single-sourced. All available order dates are included.

# COMMAND ----------

headline = spark.sql(f"""
    SELECT count(*) AS parts_on_offer,
           sum(CASE WHEN ordered_supplier_count > 0 THEN 1 ELSE 0 END) AS ordered_parts,
           sum(CASE WHEN is_single_sourced THEN 1 ELSE 0 END) AS single_sourced_parts,
           sum(CASE WHEN sourcing_status = 'SINGLE_NO_ALTERNATIVE' THEN 1 ELSE 0 END)
               AS parts_with_no_alternative,
           sum(CASE WHEN is_single_sourced THEN spend ELSE 0 END) AS single_sourced_spend,
           sum(spend) AS total_spend
    FROM {sourcing}
""")
display(headline)

h = headline.first()
ordered = h["ordered_parts"] or 0
single = h["single_sourced_parts"] or 0
print(f"Single-sourced parts: {single:,} of {ordered:,} ordered parts"
      + (f" ({single / ordered:.4%})" if ordered else ""))
if h["total_spend"]:
    print(f"Spend through them:   {float(h['single_sourced_spend'] / h['total_spend']):.4%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### How many suppliers does each part actually use?
# MAGIC
# MAGIC The distribution shows whether a low single-sourcing count is real or a bug:
# MAGIC if almost every part is ordered from all of its listed suppliers, single sourcing
# MAGIC is rare by construction.

# COMMAND ----------

distribution = spark.sql(f"""
    SELECT available_supplier_count, ordered_supplier_count, count(*) AS parts
    FROM {sourcing}
    GROUP BY available_supplier_count, ordered_supplier_count
    ORDER BY available_supplier_count, ordered_supplier_count
""")
display(distribution)

dist_rows = spark.sql(f"""
    SELECT ordered_supplier_count, count(*) AS parts
    FROM {sourcing} GROUP BY ordered_supplier_count ORDER BY ordered_supplier_count
""").collect()

if dist_rows:
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar([str(r["ordered_supplier_count"]) for r in dist_rows],
           [r["parts"] for r in dist_rows], color="#1f6f78")
    ax.set_xlabel("Suppliers actually ordered from")
    ax.set_ylabel("Number of parts")
    ax.set_yscale("log")
    ax.set_title("Parts by number of suppliers used (log scale)")
    ax.grid(axis="y", alpha=0.2)
    fig.tight_layout()
    display(fig)
    plt.close(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Where is the risk concentrated?
# MAGIC
# MAGIC Concentration means the single-sourced parts lean on one region, nation, brand
# MAGIC or supplier **more than spend overall does**. So each grouping is shown next to
# MAGIC its share of all spend: a region holding 20% of single-sourced parts and 20% of
# MAGIC spend is not a concentration; 60% against 20% is.

# COMMAND ----------

by_region = spark.sql(f"""
    WITH single AS (
        SELECT sole_supplier_region AS region, count(*) AS single_sourced_parts,
               sum(spend) AS single_sourced_spend
        FROM {sourcing} WHERE is_single_sourced GROUP BY sole_supplier_region
    ),
    overall AS (
        SELECT region, sum(spend) AS spend FROM {monthly} GROUP BY region
    )
    SELECT o.region,
           coalesce(s.single_sourced_parts, 0) AS single_sourced_parts,
           coalesce(s.single_sourced_parts, 0) / sum(s.single_sourced_parts) OVER ()
               AS share_of_single_sourced,
           o.spend / sum(o.spend) OVER () AS share_of_all_spend
    FROM overall o LEFT JOIN single s USING (region)
    ORDER BY share_of_single_sourced DESC NULLS LAST, o.region
""")
display(by_region)

for column, label in (
    ("sole_supplier_nation", "nation"),
    ("brand", "brand"),
    ("sole_supplier_name", "supplier"),
):
    print(f"Top {label}s by single-sourced parts")
    display(spark.sql(f"""
        SELECT {column} AS {label}, count(*) AS single_sourced_parts, sum(spend) AS spend
        FROM {sourcing} WHERE is_single_sourced
        GROUP BY {column} ORDER BY single_sourced_parts DESC, {label} LIMIT 10
    """))

# COMMAND ----------

region_rows = by_region.collect()
if single and region_rows:
    labels = [r["region"] for r in region_rows]
    positions = range(len(labels))
    fig, ax = plt.subplots(figsize=(9, 4))
    ax.barh([p - 0.2 for p in positions],
            [float(r["share_of_single_sourced"] or 0) for r in region_rows],
            height=0.4, color="#c45c36", label="Share of single-sourced parts")
    ax.barh([p + 0.2 for p in positions],
            [float(r["share_of_all_spend"] or 0) for r in region_rows],
            height=0.4, color="#1f6f78", label="Share of all spend")
    ax.set_yticks(list(positions), labels)
    ax.invert_yaxis()
    ax.xaxis.set_major_formatter(PercentFormatter(1.0))
    ax.set_title("Single-sourced parts by the sole supplier's region")
    ax.legend(loc="lower right")
    ax.grid(axis="x", alpha=0.2)
    fig.tight_layout()
    display(fig)
    plt.close(fig)
else:
    print("No single-sourced parts, so there is no regional concentration to chart.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q4. Suppliers with customer complaints in free text
# MAGIC
# MAGIC There is no complaint column. The signal is in `s_comment`. TPC-H writes
# MAGIC complaints as *"Customer ... Complaints"*, and recommendations as
# MAGIC *"Customer ... Recommends"*. The TPC-H spec's own Query 16 excludes suppliers
# MAGIC with `s_comment LIKE '%Customer%Complaints%'`, and gold uses the same pattern.
# MAGIC
# MAGIC Before trusting a text rule, profile it. The cell below compares the chosen
# MAGIC pattern with looser and stricter alternatives.

# COMMAND ----------

supplier = config.table("silver", "supplier")
display(spark.sql(f"""
    SELECT
      count_if(s_comment LIKE '{COMPLAINT_PATTERN}')     AS customer_then_complaints,
      count_if(s_comment LIKE '%Complaints%')            AS any_complaints_word,
      count_if(lower(s_comment) LIKE '%complain%')       AS any_case_complain,
      count_if(s_comment LIKE '%Customer%Recommends%')   AS customer_then_recommends,
      count_if(s_comment LIKE '%Customer%')              AS any_customer_word,
      count(*)                                           AS suppliers
    FROM {supplier}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC If `any_case_complain` is larger than `customer_then_complaints`, the extra rows
# MAGIC are ordinary random comment text that happens to contain the word. The reference
# MAGIC data uses "complaints" only inside the planted pattern. Read the extra rows below
# MAGIC before choosing a looser rule.

# COMMAND ----------

display(spark.sql(f"""
    SELECT s_suppkey, s_comment FROM {supplier}
    WHERE lower(s_comment) LIKE '%complain%' AND NOT s_comment LIKE '{COMPLAINT_PATTERN}'
    LIMIT 20
"""))

# COMMAND ----------

flagged = spark.sql(f"""
    SELECT supplier_key, supplier_name, nation, region, supplier_comment,
           line_count, spend, spend_share
    FROM {complaints} WHERE has_complaint
    ORDER BY spend DESC, supplier_key
""")
display(flagged)

totals = spark.sql(f"""
    SELECT count_if(has_complaint) AS complaint_suppliers,
           count(*) AS suppliers,
           sum(CASE WHEN has_complaint THEN spend ELSE 0 END) AS complaint_spend,
           sum(spend) AS total_spend
    FROM {complaints}
""").first()
supplier_share = totals["complaint_suppliers"] / totals["suppliers"] if totals["suppliers"] else 0
spend_share = (float(totals["complaint_spend"] / totals["total_spend"])
               if totals["total_spend"] else None)
print(f"Suppliers with complaints: {totals['complaint_suppliers']:,} of {totals['suppliers']:,}"
      f" ({supplier_share:.4%})")
if spend_share is not None:
    print(f"Share of spend through them: {spend_share:.4%}")
    if supplier_share:
        print(f"That is {spend_share / supplier_share:.2f}x their share of the supplier count.")

# COMMAND ----------

flag_rows = flagged.limit(25).collect()
if flag_rows:
    fig, ax = plt.subplots(figsize=(10, max(3, 0.3 * len(flag_rows))))
    ax.barh([r["supplier_name"] for r in flag_rows],
            [float(r["spend"]) / 1_000_000 for r in flag_rows], color="#c45c36")
    ax.invert_yaxis()
    ax.set_xlabel("Spend (million currency units)")
    share_label = f" - {spend_share:.3%} of all spend" if spend_share is not None else ""
    ax.set_title(f"Spend through suppliers with complaints{share_label}")
    ax.grid(axis="x", alpha=0.2)
    fig.tight_layout()
    display(fig)
    plt.close(fig)
else:
    print("No supplier comment matches the complaint pattern.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Is that share stable over time?
# MAGIC
# MAGIC Monthly spend comes from `supplier_spend_monthly`, so it uses the same spend and
# MAGIC period definitions as monitoring. The first and last months of the data may be
# MAGIC partial, so read their values with care.

# COMMAND ----------

trend = spark.sql(f"""
    SELECT m.period,
           sum(CASE WHEN c.has_complaint THEN m.spend ELSE 0 END) / sum(m.spend)
               AS complaint_spend_share
    FROM {monthly} m JOIN {complaints} c USING (supplier_key)
    GROUP BY m.period ORDER BY m.period
""")
trend_rows = trend.collect()
if trend_rows:
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot([r["period"] for r in trend_rows],
            [float(r["complaint_spend_share"]) for r in trend_rows], color="#c45c36")
    if spend_share is not None:
        ax.axhline(spend_share, color="#9ca3af", linestyle="--", label="All-time share")
        ax.legend()
    ax.yaxis.set_major_formatter(PercentFormatter(1.0, decimals=2))
    ax.set_title("Monthly share of spend through suppliers with complaints")
    ax.grid(alpha=0.2)
    fig.tight_layout()
    display(fig)
    plt.close(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interpretation
# MAGIC
# MAGIC **Q3.** Single sourcing is a choice risk, not a supply risk: an alternative exists
# MAGIC but nobody has ordered from it, so it has no order history, no agreed delivery
# MAGIC terms, and possibly no working contact. If the sole supplier fails, switching
# MAGIC takes time. Parts with no alternative at all are a different risk and are shown
# MAGIC separately. A region, brand or supplier is a concentration only when its share
# MAGIC of single-sourced parts clearly exceeds its share of overall spend.
# MAGIC
# MAGIC **Q4.** The complaint rule follows the TPC-H generator and Query 16. The share
# MAGIC compares spend with the number of suppliers: a share of spend close to their share
# MAGIC of the supplier count means complaints do not change where we buy. A higher share
# MAGIC would mean we keep buying from them despite the complaints. Complaints carry no
# MAGIC date, so the time series shows how much we buy from those suppliers, not when the
# MAGIC complaints were made.
