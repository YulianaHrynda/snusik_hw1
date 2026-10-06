"""Resolved names for every table the pipeline touches.

No module outside this one spells out a catalog, schema or table name. They ask
a :class:`Config` instead, so the same code runs against a dev schema and
against pre-production by changing parameters only.

Precedence, lowest to highest: ``defaults`` in ``conf/config.yaml``, the
``environments.<env>`` block, ``TPCH_*`` environment variables, explicit
keyword overrides (the command-line flags).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

LAYERS = ("bronze", "silver", "gold")

_ENV_PREFIX = "TPCH_"
_OVERRIDABLE = ("catalog", "schema_prefix", "source_catalog", "source_schema", "write_mode")

#: The repo-root copy is what you edit; the second is the same file carried
#: inside the wheel, so a job does not need the repo checked out beside it.
_CONFIG_CANDIDATES = (
    Path(__file__).resolve().parents[2] / "conf" / "config.yaml",
    Path(__file__).resolve().parent / "conf" / "config.yaml",
)


class ConfigError(RuntimeError):
    """The configuration is missing, unknown or internally inconsistent."""


@dataclass(frozen=True)
class Config:
    """Build it with :func:`load_config`, never by hand."""

    catalog: str
    schema_prefix: str
    source_catalog: str
    source_schema: str
    tables: tuple[str, ...]
    write_mode: str
    env: str

    def schema(self, layer: str) -> str:
        """Bare schema name, e.g. ``tpch_procurement_bronze``."""
        if layer not in LAYERS:
            raise ConfigError(f"unknown layer {layer!r}; expected one of {list(LAYERS)}")
        return f"{self.schema_prefix}_{layer}"

    def fq_schema(self, layer: str) -> str:
        """Catalog-qualified schema, e.g. ``workspace.tpch_procurement_bronze``."""
        return f"{self.catalog}.{self.schema(layer)}"

    def table(self, layer: str, name: str) -> str:
        """Three-level name of a table we write."""
        return f"{self.fq_schema(layer)}.{name}"

    def source_table(self, name: str) -> str:
        """Three-level name of a table we read, e.g. ``samples.tpch.supplier``."""
        return f"{self.source_catalog}.{self.source_schema}.{name}"

    def create_schemas(self, spark: Any) -> None:
        for layer in LAYERS:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.fq_schema(layer)}")

    def describe(self) -> str:
        """One-screen summary; every entry point logs this before doing work."""
        return "\n".join(
            [
                f"env        : {self.env}",
                f"source     : {self.source_catalog}.{self.source_schema}",
                f"write mode : {self.write_mode}",
                f"tables     : {', '.join(self.tables)}",
                *(f"{layer:<11}: {self.fq_schema(layer)}" for layer in LAYERS),
            ]
        )


def _config_path() -> Path:
    override = os.environ.get(f"{_ENV_PREFIX}CONFIG_PATH")
    if override:
        return Path(override)
    for candidate in _CONFIG_CANDIDATES:
        if candidate.exists():
            return candidate
    raise ConfigError(f"no config file at {' or '.join(str(c) for c in _CONFIG_CANDIDATES)}")


def load_config(
    env: str | None = None,
    *,
    config_path: str | Path | None = None,
    **overrides: Any,
) -> Config:
    """Resolve the configuration for one pipeline run.

    ``None`` overrides are ignored, so unset argparse flags pass straight through.
    """
    path = Path(config_path) if config_path else _config_path()
    with path.open() as handle:
        document = yaml.safe_load(handle) or {}

    env = env or os.environ.get(f"{_ENV_PREFIX}ENV") or "dev"
    environments = document.get("environments") or {}
    if env not in environments:
        raise ConfigError(f"unknown environment {env!r}; defined: {sorted(environments)}")

    settings: dict[str, Any] = dict(document.get("defaults") or {})
    settings.update(environments[env] or {})
    settings.update(
        {k: os.environ[f"{_ENV_PREFIX}{k.upper()}"] for k in _OVERRIDABLE
         if os.environ.get(f"{_ENV_PREFIX}{k.upper()}")}
    )
    settings.update({k: v for k, v in overrides.items() if v is not None})

    missing = [k for k in (*_OVERRIDABLE, "tables") if not settings.get(k)]
    if missing:
        raise ConfigError(f"missing config keys: {', '.join(missing)}")

    return Config(
        catalog=settings["catalog"],
        schema_prefix=settings["schema_prefix"],
        source_catalog=settings["source_catalog"],
        source_schema=settings["source_schema"],
        tables=tuple(settings["tables"]),
        write_mode=settings["write_mode"],
        env=env,
    )
