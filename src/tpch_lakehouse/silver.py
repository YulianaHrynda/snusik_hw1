"""Silver layer — 3NF model with enforced data quality.

Owner: Person 2.

Reads ``config.table("bronze", <name>)`` for every table in ``config.tables``.
Writes conformed tables plus a ``quarantine_<name>`` holding the rows that
failed a rule, with the reason attached.

Column names stay as they are in TPC-H. Monitoring and gold already join on
``l_partkey``, ``ps_supplycost``, ``s_nationkey`` and the rest; renaming them
here would silently break those queries.

The model is already in third normal form, so silver keeps the same eight
tables at the same grain. It does not aggregate and it does not flatten a
supplier onto its nation. Primary and foreign keys are declared afterwards
when the table is Delta. Those catalog constraints are informational, so the
rules below are what actually keep a bad row out of silver.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame

from tpch_lakehouse.cli import run_layer
from tpch_lakehouse.config import Config

LAYER = "silver"

#: Bronze's contract. Carried through so a quarantined row can still say
#: which feed it came from.
METADATA_COLUMNS = ("_ingested_at", "_source")

#: The four checks the Procurement profile asks to be shown. Logged on every
#: run so the validation demo has one place to point at.
HEADLINE_CHECKS = (
    ("fk_lineitem_partsupp", "lineitem"),
    ("fk_supplier_nation", "supplier"),
    ("fk_nation_region", "nation"),
    ("supplycost_within_retail", "partsupp"),
)

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ForeignKey:
    """One referential rule, enforced with an anti-join against silver.

    ``columns`` is the whole key. A two-column key is one join on both
    columns, not two joins.
    """

    name: str
    columns: tuple[str, ...]
    parent: str
    parent_columns: tuple[str, ...]


@dataclass(frozen=True)
class Table:
    name: str
    pk: tuple[str, ...]
    columns: tuple[tuple[str, str], ...]
    fks: tuple[ForeignKey, ...] = ()
    #: partsupp only. ``0 < ps_supplycost <= p_retailprice`` of that part.
    check_supply_cost: bool = False


def _cols(*pairs: str) -> tuple[tuple[str, str], ...]:
    if len(pairs) % 2:
        raise RuntimeError("column specs must be name, type, name, type, ...")
    return tuple((pairs[i], pairs[i + 1]) for i in range(0, len(pairs), 2))


# Parents before children, same order as conf/config.yaml. A foreign key is
# checked against the parent's silver rows, so a nation that failed its own
# region check cannot be referenced by a supplier.
TABLES: tuple[Table, ...] = (
    Table(
        "region",
        pk=("r_regionkey",),
        columns=_cols(
            "r_regionkey",
            "bigint",
            "r_name",
            "string",
            "r_comment",
            "string",
        ),
    ),
    Table(
        "nation",
        pk=("n_nationkey",),
        columns=_cols(
            "n_nationkey",
            "bigint",
            "n_name",
            "string",
            "n_regionkey",
            "bigint",
            "n_comment",
            "string",
        ),
        fks=(ForeignKey("fk_nation_region", ("n_regionkey",), "region", ("r_regionkey",)),),
    ),
    Table(
        "customer",
        pk=("c_custkey",),
        columns=_cols(
            "c_custkey",
            "bigint",
            "c_name",
            "string",
            "c_address",
            "string",
            "c_nationkey",
            "bigint",
            "c_phone",
            "string",
            "c_acctbal",
            "decimal(15,2)",
            "c_mktsegment",
            "string",
            "c_comment",
            "string",
        ),
        fks=(ForeignKey("fk_customer_nation", ("c_nationkey",), "nation", ("n_nationkey",)),),
    ),
    Table(
        "supplier",
        pk=("s_suppkey",),
        columns=_cols(
            "s_suppkey",
            "bigint",
            "s_name",
            "string",
            "s_address",
            "string",
            "s_nationkey",
            "bigint",
            "s_phone",
            "string",
            "s_acctbal",
            "decimal(15,2)",
            "s_comment",
            "string",
        ),
        fks=(ForeignKey("fk_supplier_nation", ("s_nationkey",), "nation", ("n_nationkey",)),),
    ),
    Table(
        "part",
        pk=("p_partkey",),
        columns=_cols(
            "p_partkey",
            "bigint",
            "p_name",
            "string",
            "p_mfgr",
            "string",
            "p_brand",
            "string",
            "p_type",
            "string",
            "p_size",
            "int",
            "p_container",
            "string",
            "p_retailprice",
            "decimal(15,2)",
            "p_comment",
            "string",
        ),
    ),
    Table(
        "partsupp",
        pk=("ps_partkey", "ps_suppkey"),
        columns=_cols(
            "ps_partkey",
            "bigint",
            "ps_suppkey",
            "bigint",
            "ps_availqty",
            "int",
            "ps_supplycost",
            "decimal(15,2)",
            "ps_comment",
            "string",
        ),
        fks=(
            ForeignKey("fk_partsupp_part", ("ps_partkey",), "part", ("p_partkey",)),
            ForeignKey("fk_partsupp_supplier", ("ps_suppkey",), "supplier", ("s_suppkey",)),
        ),
        check_supply_cost=True,
    ),
    Table(
        "orders",
        pk=("o_orderkey",),
        columns=_cols(
            "o_orderkey",
            "bigint",
            "o_custkey",
            "bigint",
            "o_orderstatus",
            "string",
            "o_totalprice",
            "decimal(15,2)",
            "o_orderdate",
            "date",
            "o_orderpriority",
            "string",
            "o_clerk",
            "string",
            "o_shippriority",
            "int",
            "o_comment",
            "string",
        ),
        fks=(ForeignKey("fk_orders_customer", ("o_custkey",), "customer", ("c_custkey",)),),
    ),
    Table(
        "lineitem",
        pk=("l_orderkey", "l_linenumber"),
        columns=_cols(
            "l_orderkey",
            "bigint",
            "l_partkey",
            "bigint",
            "l_suppkey",
            "bigint",
            "l_linenumber",
            "int",
            "l_quantity",
            "decimal(15,2)",
            "l_extendedprice",
            "decimal(15,2)",
            "l_discount",
            "decimal(15,2)",
            "l_tax",
            "decimal(15,2)",
            "l_returnflag",
            "string",
            "l_linestatus",
            "string",
            "l_shipdate",
            "date",
            "l_commitdate",
            "date",
            "l_receiptdate",
            "date",
            "l_shipinstruct",
            "string",
            "l_shipmode",
            "string",
            "l_comment",
            "string",
        ),
        fks=(
            ForeignKey("fk_lineitem_order", ("l_orderkey",), "orders", ("o_orderkey",)),
            # Both columns together. Joining on the part alone would count a
            # supplier who does not sell that part.
            ForeignKey(
                "fk_lineitem_partsupp",
                ("l_partkey", "l_suppkey"),
                "partsupp",
                ("ps_partkey", "ps_suppkey"),
            ),
        ),
    ),
)

SPECS: dict[str, Table] = {table.name: table for table in TABLES}


def quarantine_table(table: str) -> str:
    return f"quarantine_{table}"


def anti_join_missing(
    child: DataFrame,
    parent: DataFrame,
    child_columns: tuple[str, ...],
    parent_columns: tuple[str, ...],
) -> DataFrame:
    """Rows in ``child`` whose key is not present in ``parent``.

    ``left_anti`` on every column of the key at once. For lineitem that key
    is ``(l_partkey, l_suppkey)``.
    """
    if len(child_columns) != len(parent_columns):
        raise RuntimeError(f"foreign key width mismatch: {child_columns} vs {parent_columns}")
    parent_keys = parent.select(
        *[
            F.col(parent_column).alias(child_column)
            for child_column, parent_column in zip(child_columns, parent_columns, strict=True)
        ]
    ).dropDuplicates(list(child_columns))
    return child.join(parent_keys, list(child_columns), "left_anti")


def _typed_column(name: str, dtype: str) -> Any:
    casted = F.col(name).cast(dtype)
    if dtype == "string":
        casted = F.trim(casted)
    return casted.alias(name)


def _read_bronze(spark: Any, config: Config, spec: Table) -> DataFrame:
    source = config.table("bronze", spec.name)
    bronze = spark.table(source)
    missing = [name for name in METADATA_COLUMNS if name not in bronze.columns]
    if missing:
        raise RuntimeError(
            f"{source} is missing {missing}; silver expects them on every bronze table"
        )
    columns = [_typed_column(name, dtype) for name, dtype in spec.columns]
    metadata = [F.col(name) for name in METADATA_COLUMNS]
    return bronze.select(*columns, *metadata)


def _split_primary_key(df: DataFrame, pk: tuple[str, ...]) -> tuple[DataFrame, DataFrame]:
    """One survivor per key. Null keys and the extra copies are quarantined.

    The survivor is the copy with the latest ``_ingested_at``, so a rerun
    that delivered the same key twice keeps the newest feed.
    """
    null_key = F.lit(False)
    for column in pk:
        null_key = null_key | F.col(column).isNull()

    nulls = df.filter(null_key).withColumn("_failed_rules", F.lit("primary_key_not_null"))
    keyed = df.filter(~null_key)
    ranked = keyed.withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy(*pk).orderBy(F.col("_ingested_at").desc_nulls_last())
        ),
    )
    survivor = ranked.filter(F.col("_rn") == 1).drop("_rn")
    duplicates = (
        ranked.filter(F.col("_rn") > 1)
        .drop("_rn")
        .withColumn("_failed_rules", F.lit("duplicate_primary_key"))
    )
    return survivor, nulls.unionByName(duplicates)


def _supply_cost_violations(partsupp: DataFrame, part: DataFrame) -> DataFrame:
    """Agreements where ``0 < ps_supplycost <= p_retailprice`` is not true.

    A missing part fails the rule as well: the comparison cannot be shown.
    The part foreign key reports that row too, under its own reason.
    A rule that comes back NULL is a failure. Filtering on ``~condition``
    alone drops those rows from both sides, and they vanish.
    """
    prices = part.select(
        F.col("p_partkey").alias("_partkey"),
        F.col("p_retailprice").alias("_retail"),
    )
    compared = partsupp.alias("ps").join(
        prices.alias("pr"),
        F.col("ps.ps_partkey") == F.col("pr._partkey"),
        "left",
    )
    within_retail = (
        F.col("ps.ps_supplycost").isNotNull()
        & (F.col("ps.ps_supplycost") > F.lit(0))
        & F.col("pr._retail").isNotNull()
        & (F.col("ps.ps_supplycost") <= F.col("pr._retail"))
    )
    keep = [F.col(f"ps.{column}").alias(column) for column in partsupp.columns]
    return compared.filter(~F.coalesce(within_retail, F.lit(False))).select(*keep)


def _failed_keys(failures: list[tuple[str, DataFrame]], pk: tuple[str, ...]) -> DataFrame:
    pieces = [
        failing.select(*pk).dropDuplicates(list(pk)).withColumn("_reason", F.lit(name))
        for name, failing in failures
    ]
    stacked = pieces[0]
    for piece in pieces[1:]:
        stacked = stacked.unionByName(piece)
    return stacked.groupBy(*pk).agg(
        F.concat_ws(",", F.array_sort(F.collect_set("_reason"))).alias("_failed_rules")
    )


def _apply_rules(
    survivor: DataFrame,
    spec: Table,
    parents: dict[str, DataFrame],
) -> tuple[DataFrame, DataFrame]:
    failures: list[tuple[str, DataFrame]] = []
    for fk in spec.fks:
        orphans = anti_join_missing(survivor, parents[fk.parent], fk.columns, fk.parent_columns)
        failures.append((fk.name, orphans))
    if spec.check_supply_cost:
        failures.append(
            ("supplycost_within_retail", _supply_cost_violations(survivor, parents["part"]))
        )

    if not failures:
        empty = survivor.limit(0).withColumn("_failed_rules", F.lit(None).cast("string"))
        return survivor, empty

    reasons = _failed_keys(failures, spec.pk)
    tagged = survivor.join(reasons, list(spec.pk), "left")
    good = tagged.filter(F.col("_failed_rules").isNull()).drop("_failed_rules")
    bad = tagged.filter(F.col("_failed_rules").isNotNull())
    return good, bad


def conform(
    bronze: DataFrame,
    spec: Table,
    parents: dict[str, DataFrame],
) -> tuple[DataFrame, DataFrame]:
    """Type, deduplicate and split one table into (silver, quarantine)."""
    survivor, rejected = _split_primary_key(bronze, spec.pk)
    good, failed_rules = _apply_rules(survivor, spec, parents)
    quarantine = rejected.unionByName(failed_rules).withColumn(
        "_quarantined_at", F.current_timestamp()
    )
    return good, quarantine


def _write(df: DataFrame, config: Config, name: str) -> None:
    target = config.table(LAYER, name)
    writer = df.write.mode(config.write_mode)
    if config.write_mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(target)


def _quote_ident(ident: str) -> str:
    return "`" + ident.replace("`", "``") + "`"


def _quote_table(name: str) -> str:
    return ".".join(_quote_ident(part) for part in name.split("."))


def _quote_columns(columns: tuple[str, ...]) -> str:
    return ", ".join(_quote_ident(column) for column in columns)


def constraint_statements(config: Config) -> list[str]:
    """SQL that documents the model. Enforcement is the anti-join, not these."""
    statements: list[str] = []
    for spec in TABLES:
        if spec.name not in config.tables:
            continue
        target = _quote_table(config.table(LAYER, spec.name))
        for column in spec.pk:
            statements.append(
                f"ALTER TABLE {target} ALTER COLUMN {_quote_ident(column)} SET NOT NULL"
            )
        statements.append(f"ALTER TABLE {target} DROP CONSTRAINT IF EXISTS pk_{spec.name}")
        statements.append(
            f"ALTER TABLE {target} ADD CONSTRAINT pk_{spec.name} "
            f"PRIMARY KEY ({_quote_columns(spec.pk)})"
        )
    for spec in TABLES:
        if spec.name not in config.tables:
            continue
        child = _quote_table(config.table(LAYER, spec.name))
        for fk in spec.fks:
            parent = _quote_table(config.table(LAYER, fk.parent))
            statements.append(f"ALTER TABLE {child} DROP CONSTRAINT IF EXISTS {fk.name}")
            statements.append(
                f"ALTER TABLE {child} ADD CONSTRAINT {fk.name} "
                f"FOREIGN KEY ({_quote_columns(fk.columns)}) "
                f"REFERENCES {parent} ({_quote_columns(fk.parent_columns)})"
            )
    return statements


def _table_provider(spark: Any, table: str) -> str:
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect()
    for row in rows:
        if str(row["col_name"]).strip().lower() == "provider":
            return str(row["data_type"]).strip().lower()
    raise RuntimeError(f"could not read the storage provider of {table}")


def declare_keys(spark: Any, config: Config) -> None:
    """Declare primary and foreign keys on Delta tables.

    Databricks does not enforce them. On any other format we skip the DDL
    and say so: the quarantine split has already done the enforcement.
    """
    first = config.table(LAYER, config.tables[0])
    provider = _table_provider(spark, first)
    if provider != "delta":
        log.info(
            "SQL primary and foreign keys skipped for %s (storage is %s). "
            "The anti-joins are what enforce the keys.",
            first,
            provider,
        )
        return
    for statement in constraint_statements(config):
        spark.sql(statement)
        log.info("constraint: %s", statement)


def headline_results(spark: Any, config: Config) -> list[tuple[str, int]]:
    """How many quarantined rows failed each Procurement check."""
    results = []
    for rule, table in HEADLINE_CHECKS:
        quarantined = spark.table(config.table(LAYER, quarantine_table(table)))
        count = quarantined.filter(
            F.array_contains(F.split(F.col("_failed_rules"), ","), F.lit(rule))
        ).count()
        results.append((rule, count))
    return results


def run(spark: Any, config: Config) -> None:
    """Build every silver table, then declare keys and log the four checks."""
    unknown = [name for name in config.tables if name not in SPECS]
    if unknown:
        raise RuntimeError(f"no silver spec for {', '.join(unknown)}")

    config.create_schemas(spark)
    parents: dict[str, DataFrame] = {}
    cached: list[DataFrame] = []
    try:
        for name in config.tables:
            spec = SPECS[name]
            good, bad = conform(_read_bronze(spark, config, spec), spec, parents)
            good = good.cache()
            bad = bad.cache()
            cached.extend((good, bad))
            _write(good, config, spec.name)
            _write(bad, config, quarantine_table(spec.name))
            log.info(
                "%-10s %10d kept %10d quarantined -> %s",
                spec.name,
                good.count(),
                bad.count(),
                config.table(LAYER, spec.name),
            )
            parents[name] = good
        declare_keys(spark, config)
        for rule, count in headline_results(spark, config):
            log.info("check %-28s %d quarantined", rule, count)
    finally:
        for frame in cached:
            frame.unpersist()


def main() -> None:
    run_layer(__doc__, run)


if __name__ == "__main__":
    main()
