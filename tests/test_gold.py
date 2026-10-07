"""Gold Q3/Q4 marts: single-sourced parts and complaint suppliers, on a hand-built silver."""

from __future__ import annotations

import json
import pathlib
import re
from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from tpch_lakehouse import gold, monitoring
from tpch_lakehouse.config import load_config
from tpch_lakehouse.gold import part_sourcing, supplier_complaints

DASHBOARD = pathlib.Path(__file__).resolve().parents[1] / "dashboards" / "procurement.lvdash.json"

SILVER: dict[str, tuple[str, list[tuple]]] = {
    "region": (
        "r_regionkey bigint, r_name string",
        [(1, "EUROPE"), (2, "ASIA")],
    ),
    "nation": (
        "n_nationkey bigint, n_name string, n_regionkey bigint",
        [(10, "FRANCE", 1), (20, "JAPAN", 2)],
    ),
    "supplier": (
        "s_suppkey bigint, s_name string, s_nationkey bigint, s_comment string",
        [
            (100, "Supplier#100", 10, "slyly Customer final deposits Complaints haggle"),
            (101, "Supplier#101", 20, "Customer pending Recommends quickly"),
            (102, "Supplier#102", 10, None),
            (103, "Supplier#103", 20, "customer complaints about nothing"),
            (104, "Supplier#104", 10, "Customer idle Complaints"),
        ],
    ),
    "part": (
        "p_partkey bigint, p_name string, p_brand string",
        [(key, f"p{key}", f"Brand#{key}{key}") for key in (1, 2, 3, 4)],
    ),
    "partsupp": (
        "ps_partkey bigint, ps_suppkey bigint, ps_availqty int, ps_supplycost decimal(15,2)",
        [
            (1, 100, 5, Decimal("10.00")),
            (1, 101, 5, Decimal("12.00")),
            (2, 100, 5, Decimal("5.00")),
            (2, 101, 5, Decimal("6.00")),
            (2, 102, 5, Decimal("7.00")),
            (3, 103, 5, Decimal("20.00")),
            (4, 100, 5, Decimal("1.00")),
            (4, 104, 5, Decimal("1.00")),
        ],
    ),
    "orders": (
        "o_orderkey bigint, o_orderdate date",
        [(1, date(1996, 1, 10)), (2, date(1996, 2, 10))],
    ),
    "lineitem": (
        "l_orderkey bigint, l_linenumber int, l_partkey bigint, l_suppkey bigint, "
        "l_quantity decimal(15,2)",
        [
            (1, 1, 1, 101, Decimal("2.00")),
            (2, 1, 1, 101, Decimal("1.00")),
            (1, 2, 2, 100, Decimal("4.00")),
            (2, 2, 2, 102, Decimal("2.00")),
            (2, 3, 3, 103, Decimal("1.00")),
        ],
    ),
}

SPEND = {100: Decimal("20"), 101: Decimal("36"), 102: Decimal("14"), 103: Decimal("20"), 104: 0}
TOTAL_SPEND = sum(SPEND.values())


@pytest.fixture(scope="module")
def built(tmp_path_factory: pytest.TempPathFactory):
    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("gold-tests")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    config = load_config(catalog="spark_catalog", schema_prefix="p4")
    config.create_schemas(spark)
    for name, (schema, rows) in SILVER.items():
        spark.createDataFrame(rows, schema).write.mode("overwrite").saveAsTable(
            config.table("silver", name)
        )
    gold.run(spark, config)
    monitoring.run(spark, config)
    yield spark, config
    spark.stop()


def test_part_sourcing_classifies_every_part(built):
    spark, config = built
    rows = {row["part_key"]: row for row in part_sourcing(spark, config).collect()}
    assert {key: row["sourcing_status"] for key, row in rows.items()} == {
        1: "SINGLE_WITH_ALTERNATIVES",
        2: "MULTI_SUPPLIER",
        3: "SINGLE_NO_ALTERNATIVE",
        4: "NOT_ORDERED",
    }
    assert [key for key, row in rows.items() if row["is_single_sourced"]] == [1]


def test_part_sourcing_names_the_sole_supplier_only_when_there_is_one(built):
    spark, config = built
    rows = {row["part_key"]: row for row in part_sourcing(spark, config).collect()}
    single = rows[1]
    assert (single["available_supplier_count"], single["ordered_supplier_count"]) == (2, 1)
    assert single["unused_supplier_count"] == 1
    assert single["sole_supplier_key"] == 101
    assert (single["sole_supplier_nation"], single["sole_supplier_region"]) == ("JAPAN", "ASIA")
    assert single["line_count"] == 2
    assert single["spend"] == Decimal("36")
    assert rows[2]["sole_supplier_key"] is None
    assert rows[4]["sole_supplier_key"] is None


def test_part_sourcing_is_one_row_per_part(built):
    spark, config = built
    frame = part_sourcing(spark, config)
    assert frame.count() == frame.select("part_key").distinct().count() == 4


def test_supplier_complaints_match_the_tpch_pattern_only(built):
    spark, config = built
    flags = {
        row["supplier_key"]: row["has_complaint"]
        for row in supplier_complaints(spark, config).collect()
    }
    assert flags == {100: True, 101: False, 102: False, 103: False, 104: True}


def test_supplier_complaints_spend_and_share(built):
    spark, config = built
    rows = {row["supplier_key"]: row for row in supplier_complaints(spark, config).collect()}
    assert {key: row["spend"] for key, row in rows.items()} == SPEND
    assert all(row["total_spend"] == TOTAL_SPEND for row in rows.values())
    assert float(sum(row["spend_share"] for row in rows.values())) == pytest.approx(1)
    complaint_share = float(
        sum(row["spend_share"] for row in rows.values() if row["has_complaint"])
    )
    assert complaint_share == pytest.approx(20 / 90)
    assert rows[104]["line_count"] == 0


def _dashboard() -> dict:
    return json.loads(DASHBOARD.read_text())


def _dataset_sql(dataset: dict) -> str:
    return "".join(dataset["queryLines"])


def test_dashboard_queries_name_tables_without_catalog_or_schema():
    """The bundle sets dataset_catalog/dataset_schema; a qualified name would bypass it."""
    for dataset in _dashboard()["datasets"]:
        sql = _dataset_sql(dataset)
        tables = re.findall(r"\b(?:FROM|JOIN)\s+([\w.`]+)", sql, flags=re.IGNORECASE)
        assert tables, dataset["name"]
        assert all("." not in table for table in tables), (dataset["name"], tables)


def test_every_dashboard_query_runs_and_feeds_its_widgets(built):
    spark, config = built
    spark.catalog.setCurrentDatabase(config.schema("gold"))
    columns = {
        dataset["name"]: set(spark.sql(_dataset_sql(dataset)).columns)
        for dataset in _dashboard()["datasets"]
    }
    for page in _dashboard()["pages"]:
        for item in page["layout"]:
            for query in item["widget"].get("queries", []):
                name = query["query"]["datasetName"]
                assert name in columns, (item["widget"]["name"], name)
                fields = {field["name"] for field in query["query"]["fields"]}
                assert fields <= columns[name], (item["widget"]["name"], fields - columns[name])
