"""Silver keeps the TPC-H model, and the three Procurement checks actually reject rows."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from tpch_lakehouse.config import load_config
from tpch_lakehouse.silver import (
    METADATA_COLUMNS,
    SPECS,
    TABLES,
    constraint_statements,
    headline_results,
    quarantine_table,
    run,
)

TS = datetime(2026, 1, 1, 12, 0, 0)
TS_LATER = datetime(2026, 2, 1, 12, 0, 0)

DEFAULTS: dict[str, dict[str, str]] = {
    "region": {"r_regionkey": "0", "r_name": "NAME", "r_comment": "comment"},
    "nation": {
        "n_nationkey": "0",
        "n_name": "NAME",
        "n_regionkey": "1",
        "n_comment": "comment",
    },
    "customer": {
        "c_custkey": "0",
        "c_name": "Customer",
        "c_address": "addr",
        "c_nationkey": "10",
        "c_phone": "1-800",
        "c_acctbal": "100.00",
        "c_mktsegment": "BUILDING",
        "c_comment": "comment",
    },
    "supplier": {
        "s_suppkey": "0",
        "s_name": "Supplier",
        "s_address": "addr",
        "s_nationkey": "10",
        "s_phone": "1-800",
        "s_acctbal": "50.00",
        "s_comment": "comment",
    },
    "part": {
        "p_partkey": "0",
        "p_name": "part",
        "p_mfgr": "Manufacturer#1",
        "p_brand": "Brand#32",
        "p_type": "ECONOMY BRUSHED STEEL",
        "p_size": "7",
        "p_container": "SM BOX",
        "p_retailprice": "100.00",
        "p_comment": "comment",
    },
    "partsupp": {
        "ps_partkey": "0",
        "ps_suppkey": "0",
        "ps_availqty": "10",
        "ps_supplycost": "40.00",
        "ps_comment": "comment",
    },
    "orders": {
        "o_orderkey": "0",
        "o_custkey": "1",
        "o_orderstatus": "O",
        "o_totalprice": "1000.00",
        "o_orderdate": "1996-01-15",
        "o_orderpriority": "1-URGENT",
        "o_clerk": "Clerk#1",
        "o_shippriority": "0",
        "o_comment": "comment",
    },
    "lineitem": {
        "l_orderkey": "500",
        "l_partkey": "200",
        "l_suppkey": "100",
        "l_linenumber": "1",
        "l_quantity": "10.00",
        "l_extendedprice": "400.00",
        "l_discount": "0.05",
        "l_tax": "0.02",
        "l_returnflag": "N",
        "l_linestatus": "O",
        "l_shipdate": "1996-02-01",
        "l_commitdate": "1996-03-01",
        "l_receiptdate": "1996-02-15",
        "l_shipinstruct": "DELIVER IN PERSON",
        "l_shipmode": "TRUCK",
        "l_comment": "keep",
    },
}


def _record(table: str, **overrides: object) -> tuple:
    spec = SPECS[table]
    values: dict[str, object] = dict(DEFAULTS[table])
    values["_ingested_at"] = TS
    values["_source"] = f"feed.{table}"
    values.update(overrides)
    keys = [name for name, _ in spec.columns] + list(METADATA_COLUMNS)
    return tuple(values[key] for key in keys)


FIXTURES: dict[str, list[tuple]] = {
    "region": [
        _record(
            "region",
            r_regionkey="1",
            r_name=" AMERICA ",
            r_comment="newer",
            _ingested_at=TS_LATER,
        ),
        _record("region", r_regionkey="1", r_name="AMERICA", r_comment="stale"),
        _record("region", r_regionkey="2", r_name="ASIA", r_comment="asia"),
    ],
    "nation": [
        _record("nation", n_nationkey="10", n_name="UNITED STATES", n_regionkey="1"),
        _record("nation", n_nationkey="11", n_name="BRAZIL", n_regionkey="1"),
        _record("nation", n_nationkey="99", n_name="NOWHERE", n_regionkey="50"),
    ],
    "customer": [
        _record("customer", c_custkey="1", c_name="Customer#1"),
        _record("customer", c_custkey="2", c_name="Customer#2", c_nationkey="99"),
        _record("customer", c_custkey=None, c_name="Customer#null"),
    ],
    "supplier": [
        _record("supplier", s_suppkey="100", s_name="  Supplier#100  ", s_nationkey="10"),
        _record("supplier", s_suppkey="102", s_name="Supplier#102", s_nationkey="11"),
        _record("supplier", s_suppkey="101", s_name="Supplier#101", s_nationkey="77"),
    ],
    "part": [
        _record("part", p_partkey="200", p_brand="Brand#32", p_retailprice="100.00"),
        _record("part", p_partkey="201", p_brand="Brand#33", p_retailprice="50.00"),
        _record("part", p_partkey="202", p_brand="Brand#34", p_retailprice="80.00"),
    ],
    "partsupp": [
        _record("partsupp", ps_partkey="200", ps_suppkey="100", ps_supplycost="40.00"),
        # Equal to retail price. The rule is ``<=``, so this one stays.
        _record("partsupp", ps_partkey="201", ps_suppkey="100", ps_supplycost="50.00"),
        _record("partsupp", ps_partkey="200", ps_suppkey="102", ps_supplycost="150.00"),
        _record("partsupp", ps_partkey="201", ps_suppkey="999", ps_supplycost="10.00"),
        _record("partsupp", ps_partkey="998", ps_suppkey="100", ps_supplycost="10.00"),
        # A real supplier and a real part, but the cost is not positive.
        _record("partsupp", ps_partkey="202", ps_suppkey="100", ps_supplycost="0.00"),
    ],
    "orders": [
        _record("orders", o_orderkey="500", o_custkey="1"),
        _record("orders", o_orderkey="501", o_custkey="2"),
    ],
    "lineitem": [
        _record("lineitem", l_linenumber="1", l_comment="keep", _ingested_at=TS_LATER),
        _record("lineitem", l_linenumber="1", l_comment="drop"),
        # Pair exists in bronze but the agreement fails the price rule, so it
        # never becomes a silver parent.
        _record(
            "lineitem",
            l_linenumber="2",
            l_partkey="200",
            l_suppkey="102",
            l_comment="price-parent",
        ),
        # Part 201 and supplier 102 are both valid. They just never meet in partsupp.
        _record(
            "lineitem",
            l_linenumber="3",
            l_partkey="201",
            l_suppkey="102",
            l_comment="no-such-pair",
        ),
        _record(
            "lineitem",
            l_linenumber="4",
            l_partkey="201",
            l_suppkey="100",
            l_comment="boundary",
        ),
        _record("lineitem", l_orderkey="501", l_linenumber="1", l_comment="bad-order"),
    ],
}


def test_foreign_keys_point_at_a_parent_built_earlier():
    order = [table.name for table in TABLES]
    assert order == list(load_config().tables)
    for spec in TABLES:
        for fk in spec.fks:
            assert order.index(fk.parent) < order.index(spec.name)
            assert len(fk.columns) == len(fk.parent_columns)


def test_lineitem_partsupp_key_is_both_columns():
    lineitem = SPECS["lineitem"]
    fk = next(fk for fk in lineitem.fks if fk.name == "fk_lineitem_partsupp")
    assert fk.columns == ("l_partkey", "l_suppkey")
    assert (fk.parent, fk.parent_columns) == ("partsupp", ("ps_partkey", "ps_suppkey"))
    assert SPECS["partsupp"].check_supply_cost is True
    assert SPECS["part"].check_supply_cost is False


def test_constraint_statements_declare_keys_parents_first():
    config = load_config(catalog="main", schema_prefix="demo")
    sql = "\n".join(constraint_statements(config))
    partsupp_pk = sql.index("CONSTRAINT pk_partsupp PRIMARY KEY (`ps_partkey`, `ps_suppkey`)")
    lineitem_fk = sql.index(
        "CONSTRAINT fk_lineitem_partsupp FOREIGN KEY (`l_partkey`, `l_suppkey`) "
        "REFERENCES `main`.`demo_silver`.`partsupp` (`ps_partkey`, `ps_suppkey`)"
    )
    assert partsupp_pk < lineitem_fk
    assert "CONSTRAINT pk_lineitem PRIMARY KEY (`l_orderkey`, `l_linenumber`)" in sql
    assert (
        "CONSTRAINT fk_supplier_nation FOREIGN KEY (`s_nationkey`) "
        "REFERENCES `main`.`demo_silver`.`nation` (`n_nationkey`)"
    ) in sql
    assert (
        "CONSTRAINT fk_nation_region FOREIGN KEY (`n_regionkey`) "
        "REFERENCES `main`.`demo_silver`.`region` (`r_regionkey`)"
    ) in sql
    assert "SET NOT NULL" in sql


@pytest.fixture(scope="module")
def built(tmp_path_factory: pytest.TempPathFactory):
    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("silver-tests")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    config = load_config(catalog="spark_catalog", schema_prefix="p2")
    config.create_schemas(spark)
    for name, rows in FIXTURES.items():
        spec = SPECS[name]
        fields = [T.StructField(column, T.StringType(), True) for column, _ in spec.columns]
        fields.append(T.StructField("_ingested_at", T.TimestampType(), True))
        fields.append(T.StructField("_source", T.StringType(), True))
        frame = spark.createDataFrame(rows, T.StructType(fields))
        frame.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            config.table("bronze", name)
        )
    run(spark, config)
    yield spark, config
    spark.stop()


def _table(spark: SparkSession, config, layer: str, name: str):
    return spark.table(config.table(layer, name))


def _reasons(spark, config, table: str) -> dict[tuple, str]:
    spec = SPECS[table]
    frame = _table(spark, config, "silver", quarantine_table(table))
    return {
        tuple(row[column] for column in spec.pk): row["_failed_rules"] for row in frame.collect()
    }


def test_every_bronze_row_lands_in_silver_or_quarantine(built):
    spark, config = built
    for name in config.tables:
        bronze = _table(spark, config, "bronze", name).count()
        silver = _table(spark, config, "silver", name).count()
        quarantined = _table(spark, config, "silver", quarantine_table(name)).count()
        assert bronze == silver + quarantined, name


def test_duplicate_primary_key_keeps_the_latest_ingest(built):
    spark, config = built
    regions = {
        row["r_regionkey"]: row["r_comment"]
        for row in _table(spark, config, "silver", "region").collect()
    }
    assert regions == {1: "newer", 2: "asia"}
    america = _table(spark, config, "silver", "region").filter("r_regionkey = 1").first()
    assert america["r_name"] == "AMERICA"
    assert _reasons(spark, config, "region")[(1,)] == "duplicate_primary_key"


def test_supplier_resolves_to_a_nation_and_nation_to_a_region(built):
    spark, config = built
    nations = {row["n_nationkey"] for row in _table(spark, config, "silver", "nation").collect()}
    suppliers = {row["s_suppkey"] for row in _table(spark, config, "silver", "supplier").collect()}
    assert nations == {10, 11}
    assert suppliers == {100, 102}
    assert _reasons(spark, config, "nation")[(99,)] == "fk_nation_region"
    assert _reasons(spark, config, "supplier")[(101,)] == "fk_supplier_nation"
    kept = _table(spark, config, "silver", "supplier").filter("s_suppkey = 100").first()
    assert kept["s_name"] == "Supplier#100"


def test_null_primary_key_is_quarantined(built):
    spark, config = built
    reasons = _reasons(spark, config, "customer")
    assert reasons[(None,)] == "primary_key_not_null"
    assert reasons[(2,)] == "fk_customer_nation"
    kept = {row["c_custkey"] for row in _table(spark, config, "silver", "customer").collect()}
    assert kept == {1}


def test_supply_cost_must_be_positive_and_within_retail_price(built):
    spark, config = built
    kept = {
        (row["ps_partkey"], row["ps_suppkey"]): row["ps_supplycost"]
        for row in _table(spark, config, "silver", "partsupp").collect()
    }
    assert set(kept) == {(200, 100), (201, 100)}
    assert kept[(201, 100)] == Decimal("50.00")
    reasons = _reasons(spark, config, "partsupp")
    assert reasons[(200, 102)] == "supplycost_within_retail"
    assert reasons[(202, 100)] == "supplycost_within_retail"
    assert reasons[(201, 999)] == "fk_partsupp_supplier"
    assert reasons[(998, 100)] == "fk_partsupp_part,supplycost_within_retail"


def test_lineitem_part_supplier_pair_must_exist_in_partsupp(built):
    spark, config = built
    silver = {
        (row["l_orderkey"], row["l_linenumber"]): row["l_comment"]
        for row in _table(spark, config, "silver", "lineitem").collect()
    }
    assert silver == {(500, 1): "keep", (500, 4): "boundary"}
    reasons = _reasons(spark, config, "lineitem")
    assert reasons[(500, 1)] == "duplicate_primary_key"
    assert reasons[(500, 2)] == "fk_lineitem_partsupp"
    assert reasons[(500, 3)] == "fk_lineitem_partsupp"
    assert reasons[(501, 1)] == "fk_lineitem_order"
    # Supplier 102 and part 201 are both in silver. The missing fact is the pair.
    assert _table(spark, config, "silver", "supplier").filter("s_suppkey = 102").count() == 1
    assert _table(spark, config, "silver", "part").filter("p_partkey = 201").count() == 1


def test_headline_checks_count_the_procurement_rules(built):
    spark, config = built
    assert dict(headline_results(spark, config)) == {
        "fk_lineitem_partsupp": 2,
        "fk_supplier_nation": 1,
        "fk_nation_region": 1,
        "supplycost_within_retail": 3,
    }


def test_silver_keeps_tpch_column_names(built):
    spark, config = built
    columns = set(_table(spark, config, "silver", "supplier").columns)
    assert {"s_suppkey", "s_name", "s_nationkey", "s_comment", "_ingested_at", "_source"} <= columns
    assert "supplier_id" not in columns
    quarantine_columns = set(_table(spark, config, "silver", quarantine_table("supplier")).columns)
    assert {"_failed_rules", "_quarantined_at"} <= quarantine_columns
    assert "_failed_rules" not in columns
