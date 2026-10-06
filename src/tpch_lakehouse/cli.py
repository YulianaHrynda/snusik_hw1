"""The flags every layer takes, and the boilerplate every layer repeats."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable
from typing import Any

from tpch_lakehouse.config import Config, load_config
from tpch_lakehouse.session import get_spark


def run_layer(doc: str, run: Callable[[Any, Config], None]) -> None:
    """Parse flags, resolve the config, log it, hand a session to ``run``."""
    parser = argparse.ArgumentParser(description=doc.splitlines()[0])
    parser.add_argument("--env", help="environment block in conf/config.yaml (default: dev)")
    parser.add_argument("--config-path", help="alternative config file")
    parser.add_argument("--catalog", help="catalog to write into")
    parser.add_argument("--schema-prefix", help="prefix shared by the three layer schemas")
    parser.add_argument("--source-catalog", help="catalog to read the raw tables from")
    parser.add_argument("--source-schema", help="schema to read the raw tables from")
    parser.add_argument("--write-mode", choices=["overwrite", "append"], help="Delta write mode")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        force=True,
    )
    config = load_config(
        env=args.env,
        config_path=args.config_path,
        catalog=args.catalog,
        schema_prefix=args.schema_prefix,
        source_catalog=args.source_catalog,
        source_schema=args.source_schema,
        write_mode=args.write_mode,
    )
    logging.getLogger("tpch_lakehouse").info("%s\n%s", doc.splitlines()[0], config.describe())
    run(get_spark(), config)
