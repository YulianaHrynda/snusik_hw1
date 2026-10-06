"""Bronze layer — capture the source as is.

Owner: Person 1.

Contract for the rest of the pipeline: one bronze table per source table, same
name, same columns, same types, plus ``_ingested_at`` and ``_source``. Nothing
is cast, filtered, deduplicated or renamed here — that is silver's job.
"""

from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import functions as F

from tpch_lakehouse.cli import run_layer
from tpch_lakehouse.config import Config

LAYER = "bronze"

#: Added to every bronze table, so we can answer "what arrived, from where, when".
METADATA_COLUMNS = ("_ingested_at", "_source")

log = logging.getLogger(__name__)


def ingest_table(spark: Any, config: Config, table: str) -> int:
    """Copy one source table into bronze. Returns the row count written."""
    source = config.source_table(table)
    target = config.table(LAYER, table)

    df = (
        spark.read.table(source)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source", F.lit(source))
    )

    writer = df.write.mode(config.write_mode)
    if config.write_mode == "overwrite":
        # Without this a rerun fails the moment the source gains a column or we
        # change METADATA_COLUMNS, which is exactly when we want it to succeed.
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(target)

    return spark.read.table(target).count()


def run(spark: Any, config: Config) -> None:
    """Ingest every table in ``config.tables``."""
    config.create_schemas(spark)
    for table in config.tables:
        rows = ingest_table(spark, config, table)
        log.info("%-10s %10d rows -> %s", table, rows, config.table(LAYER, table))


def main() -> None:
    run_layer(__doc__, run)


if __name__ == "__main__":
    main()
