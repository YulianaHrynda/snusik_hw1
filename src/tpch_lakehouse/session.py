"""Getting hold of a SparkSession, on Databricks or locally."""

from __future__ import annotations

from typing import Any


def get_spark() -> Any:
    """Return the active session.

    Inside a Databricks notebook or job one already exists, and we reuse it.
    Outside, ``getOrCreate`` builds a local session so tests and dry runs work
    without a cluster.
    """
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    return SparkSession.builder.appName("tpch-procurement").getOrCreate()
