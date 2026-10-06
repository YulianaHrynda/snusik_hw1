# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — smoke run
# MAGIC
# MAGIC Clone this repo as a Git folder in Databricks, open this notebook, attach a
# MAGIC cluster and run all. No CLI and no asset bundle needed — that is for the
# MAGIC scheduled job later.
# MAGIC
# MAGIC Names come from `conf/config.yaml`. Override them in the next cell if you
# MAGIC want to write somewhere else; do not edit the pipeline code.

# COMMAND ----------

import pathlib
import sys

# The repo root is wherever pyproject.toml sits above this notebook.
REPO = next(
    p for p in [pathlib.Path.cwd(), *pathlib.Path.cwd().parents] if (p / "pyproject.toml").exists()
)
sys.path.insert(0, str(REPO / "src"))
print("repo:", REPO)

# COMMAND ----------

import logging

from tpch_lakehouse.bronze import LAYER, run
from tpch_lakehouse.config import load_config

logging.basicConfig(level="INFO", force=True)

# Pass overrides here if the defaults do not match this workspace, e.g.
#   load_config(catalog="main", schema_prefix="tpch_procurement")
config = load_config()
print(config.describe())

# COMMAND ----------

run(spark, config)  # noqa: F821 - spark is provided by Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ## What landed

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {config.fq_schema(LAYER)}"))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC The two metadata columns are the point of bronze: what arrived, from where,
# MAGIC and when. One row per source feed.

# COMMAND ----------

union = "\nUNION ALL\n".join(
    f"SELECT _source, count(*) AS rows_received, max(_ingested_at) AS last_seen "
    f"FROM {config.table(LAYER, t)} GROUP BY _source"
    for t in config.tables
)
display(spark.sql(f"{union}\nORDER BY _source"))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check: bronze matches the source, row for row
# MAGIC
# MAGIC Bronze copies, it does not filter. If any of these differ, something is wrong.

# COMMAND ----------

rows = [
    (
        t,
        spark.table(config.source_table(t)).count(),  # noqa: F821
        spark.table(config.table(LAYER, t)).count(),  # noqa: F821
    )
    for t in config.tables
]
check = spark.createDataFrame(rows, "table string, source_rows long, bronze_rows long")  # noqa: F821
display(check.withColumn("match", check.source_rows == check.bronze_rows))
