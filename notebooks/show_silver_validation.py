# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — validation
# MAGIC
# MAGIC Run this after the pipeline (person 1 runs bronze → silver → gold). The
# MAGIC notebook only reads. It is the demo: every bronze row is still accounted
# MAGIC for, and the three Procurement checks are empty on `samples.tpch`.
# MAGIC
# MAGIC The ER diagram for the slides is [`docs/silver_er.svg`](../docs/silver_er.svg).

# COMMAND ----------

import pathlib
import sys

REPO = next(
    p for p in [pathlib.Path.cwd(), *pathlib.Path.cwd().parents] if (p / "pyproject.toml").exists()
)
sys.path.insert(0, str(REPO / "src"))
print("repo:", REPO)

# COMMAND ----------

from tpch_lakehouse.config import load_config
from tpch_lakehouse.silver import LAYER, headline_results, quarantine_table

config = load_config()
print(config.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Nothing was dropped
# MAGIC
# MAGIC Silver plus its quarantine table must add back to bronze. A row that fails
# MAGIC a rule is recoverable; it is not deleted.

# COMMAND ----------

rows = []
for name in config.tables:
    bronze = spark.table(config.table("bronze", name)).count()  # noqa: F821
    silver = spark.table(config.table(LAYER, name)).count()  # noqa: F821
    quarantined = spark.table(config.table(LAYER, quarantine_table(name))).count()  # noqa: F821
    rows.append((name, bronze, silver, quarantined, bronze == silver + quarantined))

recon = spark.createDataFrame(  # noqa: F821
    rows, "table string, bronze long, silver long, quarantined long, reconciles boolean"
)
display(recon)  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## The four checks
# MAGIC
# MAGIC On a clean feed every count is zero. A non-zero count is the number of
# MAGIC rows sitting in `quarantine_<table>` with that reason.

# COMMAND ----------

summary = spark.createDataFrame(headline_results(spark, config), "check string, quarantined long")  # noqa: F821
display(summary)  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Line item → partsupp, both columns
# MAGIC
# MAGIC A part is tied to a supplier by `(partkey, suppkey)` together. This is a
# MAGIC `LEFT ANTI JOIN` on both columns: a line whose pair is not a row in
# MAGIC `partsupp` comes back here. Joining on the part alone would accept a
# MAGIC supplier who does not sell that part.
# MAGIC
# MAGIC The same anti-join is what `silver.py` applies before publishing. Parents
# MAGIC are the silver tables, so an agreement that failed its own checks (for
# MAGIC example a supply cost above retail) cannot be referenced either.

# COMMAND ----------

lineitem = config.table(LAYER, "lineitem")
partsupp = config.table(LAYER, "partsupp")

orphans = spark.sql(f"""
    SELECT l.l_orderkey, l.l_linenumber, l.l_partkey, l.l_suppkey
    FROM {lineitem} l
    LEFT ANTI JOIN {partsupp} ps
      ON ps.ps_partkey = l.l_partkey
     AND ps.ps_suppkey = l.l_suppkey
""")  # noqa: F821
print(f"line items with no partsupp row: {orphans.count()}")
display(orphans.limit(20))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Supplier → nation → region

# COMMAND ----------

supplier = config.table(LAYER, "supplier")
nation = config.table(LAYER, "nation")
region = config.table(LAYER, "region")

supplier_orphans = spark.sql(f"""
    SELECT s.s_suppkey, s.s_name, s.s_nationkey
    FROM {supplier} s
    LEFT ANTI JOIN {nation} n ON n.n_nationkey = s.s_nationkey
""")  # noqa: F821
nation_orphans = spark.sql(f"""
    SELECT n.n_nationkey, n.n_name, n.n_regionkey
    FROM {nation} n
    LEFT ANTI JOIN {region} r ON r.r_regionkey = n.n_regionkey
""")  # noqa: F821
print(f"suppliers with no nation: {supplier_orphans.count()}")
print(f"nations with no region:   {nation_orphans.count()}")
display(supplier_orphans.limit(20))  # noqa: F821
display(nation_orphans.limit(20))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Supply cost is positive and never above retail
# MAGIC
# MAGIC `0 < ps_supplycost <= p_retailprice`. A missing part fails too, because
# MAGIC the comparison cannot be shown.

# COMMAND ----------

part = config.table(LAYER, "part")
partsupp = config.table(LAYER, "partsupp")

price = spark.sql(f"""
    SELECT ps.ps_partkey, ps.ps_suppkey, ps.ps_supplycost, p.p_retailprice
    FROM {partsupp} ps
    LEFT JOIN {part} p ON p.p_partkey = ps.ps_partkey
    WHERE NOT coalesce(
        ps.ps_supplycost > 0
        AND p.p_retailprice IS NOT NULL
        AND ps.ps_supplycost <= p.p_retailprice,
        false
    )
""")  # noqa: F821
print(f"supply agreements outside (0, retail]: {price.count()}")
display(price.limit(20))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## What was quarantined, and why
# MAGIC
# MAGIC Empty is the expected result on this dataset. `_failed_rules` is there so
# MAGIC a bad feed can be read without guessing.

# COMMAND ----------

for name in config.tables:
    q = spark.table(config.table(LAYER, quarantine_table(name)))  # noqa: F821
    print(f"\n{quarantine_table(name)}: {q.count()} rows")
    if q.count():
        display(q.groupBy("_failed_rules").count().orderBy("_failed_rules"))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Keys declared on the catalog
# MAGIC
# MAGIC Primary and foreign keys are part of the silver model. On Databricks they
# MAGIC are informational: the anti-joins above are what enforce them. This cell
# MAGIC reads Unity Catalog's constraint list when the workspace has one.

# COMMAND ----------

try:
    display(spark.sql(f"""
        SELECT table_name, constraint_name, constraint_type
        FROM {config.catalog}.information_schema.table_constraints
        WHERE table_schema = '{config.schema(LAYER)}'
        ORDER BY table_name, constraint_name
    """))  # noqa: F821
except Exception as exc:
    print(f"constraint list is not available in this catalog ({exc})")
