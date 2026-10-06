"""The config is the contract between three people — so it gets tests."""

from __future__ import annotations

import dataclasses

import pytest

from tpch_lakehouse.config import ConfigError, load_config


def test_defaults_resolve_to_three_level_names():
    config = load_config()
    assert config.table("bronze", "supplier") == "workspace.tpch_procurement_bronze.supplier"
    assert config.source_table("supplier") == "samples.tpch.supplier"


def test_all_eight_tpch_tables_are_configured():
    assert set(load_config().tables) == {
        "region",
        "nation",
        "customer",
        "supplier",
        "part",
        "partsupp",
        "orders",
        "lineitem",
    }


def test_parents_come_before_children():
    tables = load_config().tables
    assert tables.index("nation") < tables.index("customer")
    assert tables.index("orders") < tables.index("lineitem")
    assert tables.index("part") < tables.index("partsupp")


def test_environment_block_overrides_defaults():
    assert load_config("preprod").schema_prefix == "tpch_procurement_preprod"


def test_explicit_overrides_beat_the_environment():
    config = load_config("preprod", catalog="sandbox")
    assert config.fq_schema("gold") == "sandbox.tpch_procurement_preprod_gold"


def test_env_vars_are_picked_up(monkeypatch):
    monkeypatch.setenv("TPCH_CATALOG", "from_env")
    assert load_config().catalog == "from_env"


def test_none_overrides_are_ignored():
    """Unset argparse flags arrive as None and must not blank out the config."""
    assert load_config(catalog=None).catalog == "workspace"


def test_unknown_environment_is_rejected():
    with pytest.raises(ConfigError, match="unknown environment"):
        load_config("staging")


def test_unknown_layer_is_rejected():
    with pytest.raises(ConfigError, match="unknown layer"):
        load_config().table("platinum", "supplier")


def test_config_is_immutable():
    with pytest.raises(dataclasses.FrozenInstanceError):
        load_config().catalog = "oops"  # type: ignore[misc]


def test_create_schemas_issues_one_statement_per_layer():
    class FakeSpark:
        def __init__(self):
            self.statements = []

        def sql(self, statement):
            self.statements.append(statement)

    spark = FakeSpark()
    config = load_config()
    config.create_schemas(spark)
    assert spark.statements == [
        f"CREATE SCHEMA IF NOT EXISTS {config.fq_schema(layer)}"
        for layer in ("bronze", "silver", "gold")
    ]


def test_no_hardcoded_names_outside_the_config_module():
    """The whole point of the config: grep the package and find nothing."""
    import pathlib

    import tpch_lakehouse

    package = pathlib.Path(tpch_lakehouse.__file__).parent
    offenders = []
    for path in package.glob("*.py"):
        if path.name == "config.py":
            continue
        text = path.read_text()
        for needle in ("samples.tpch", "workspace.", "tpch_procurement"):
            if needle in text:
                offenders.append(f"{path.name}: {needle}")
    assert not offenders, offenders
