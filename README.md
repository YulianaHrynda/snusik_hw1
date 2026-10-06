# TPC-H medallion lakehouse — Procurement

A bronze / silver / gold lakehouse over `samples.tpch`, built to answer the
questions the **Procurement** customer profile asks: what we pay suppliers, how
exposed we are to any single one, and which suppliers are worth keeping.

Group Assignment 1. The brief is in [`group_assignment_1.pdf`](group_assignment_1.pdf).

## Layout

```
conf/config.yaml            every catalog, schema and table name, in one place
src/tpch_lakehouse/
  config.py                 resolves that file into three-level table names
  cli.py                    flags shared by all four entry points
  session.py                SparkSession, on a cluster or locally
  bronze.py                 capture the source as is          (person 1)
  silver.py                 3NF + enforced data quality       (person 2)
  gold.py                   procurement marts                 (person 3)
  monitoring.py             metrics over time + alert         (person 1)
notebooks/run_bronze.py     run bronze from a Databricks Git folder
notebooks/show_silver_validation.py   silver checks, for the demo
docs/silver_er.svg          ER diagram of the silver tables
databricks.yml              asset bundle
resources/                  the job: bronze -> silver -> gold -> monitoring
tests/                      config tests, runnable without a cluster
data/                       lecture walkthroughs, reference only
```

## Running it

```bash
uv sync
uv run pytest
uv run ruff check .
```

Each layer is a console script and takes the same flags:

```bash
uv run bronze --catalog workspace --schema-prefix tpch_procurement
```

### On Databricks, by hand

Clone the repo as a Git folder, open `notebooks/run_bronze.py`, attach a cluster,
run all. Nothing to install. The notebook puts `src/` on the path and calls the
same `run()` the job calls.

### On Databricks, as a job

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run tpch_medallion -t dev
```

## Nothing is hardcoded

The assignment asks for scripts that port to another workspace, assuming access
to pre-production data only. So no module outside `config.py` names a catalog,
schema or table — they ask a `Config` object, which resolves names in this
order, each winning over the one above:

1. `defaults` in `conf/config.yaml`
2. the `environments.<env>` block (`dev`, `preprod`)
3. `TPCH_*` environment variables — `TPCH_CATALOG`, `TPCH_SCHEMA_PREFIX`, …
4. command-line flags — which is how Databricks job parameters arrive, so
   there is one mechanism rather than two

`tests/test_config.py::test_no_hardcoded_names_outside_the_config_module` greps
the package and fails if anyone slips one in.

Schemas are `<schema_prefix>_bronze`, `_silver` and `_gold`; with the defaults
that resolves to `workspace.tpch_procurement_bronze.supplier` and friends.

## Layer contracts

**Bronze** — one table per source table, same name, same columns, same types,
nothing cast or filtered, plus `_ingested_at` and `_source` (the full source
table name, e.g. `samples.tpch.supplier`). It exists to answer *what arrived,
from where, and when* — the question that becomes unanswerable once data is
cleaned on the way in.

**Silver** — 3NF, typed, deduplicated, rules enforced. The eight tables stay at
the original grain and keep their TPC-H column names. Rows that fail a rule go
to `quarantine_<table>` with `_failed_rules` attached rather than being dropped,
so rejected rows stay recoverable and bronze = silver + quarantine.

A key is only referenceable once it has landed in silver. A nation that failed
its region check cannot be used by a supplier. The diagram is
[`docs/silver_er.svg`](docs/silver_er.svg).

Primary and foreign keys are declared on Delta. Databricks does not enforce
those constraints, so each rule below is an anti-join (or, for the price, a
predicate). A predicate that comes back NULL counts as a failure.

- **Line item → partsupp.** Anti-join on `(l_partkey, l_suppkey)` =
  `(ps_partkey, ps_suppkey)` together. Either column on its own is not the key.
- **Supplier → nation → region.** `s_nationkey` must exist in `nation`, and
  `n_regionkey` must exist in `region`.
- **Supply cost.** `0 < ps_supplycost <= p_retailprice` of that part. A missing
  part fails the rule, because the comparison cannot be shown.

The other foreign keys of the model — customer to nation, partsupp to part and
to supplier, orders to customer, lineitem to orders — are enforced the same way.

**Gold** — marts shaped by the Procurement questions. Reads silver only.

## Metric definitions

Every headline number is defined once here, and every query in the repo uses
that definition. If a gold answer and a monitoring chart disagree, one of them
stopped following this section.

**Spend** — what we pay a supplier:

```
spend = ps_supplycost * l_quantity
```

Not `l_extendedprice`. That is what a customer pays *us*, which is a Finance
number; Procurement is asked what the company pays out. A line item resolves to
its supply agreement on **both** keys together, `(l_partkey, l_suppkey)` —
joining on the part alone multiplies spend by the number of suppliers offering
that part.

**Period** — calendar month of `o_orderdate`. `partsupp` carries no date of its
own, so every time series is anchored to when the goods were actually ordered.

**Spend concentration** — within one month, the share of total spend held by the
ten largest suppliers. Rising concentration is rising single-supplier risk.

**Average supply cost by region** — spend-weighted, per unit:

```
sum(ps_supplycost * l_quantity) / sum(l_quantity)
```

Deliberately not `avg(ps_supplycost)`. That averages a price list rather than
real purchases, and averaging those averages across regions produces a number
with no meaning — the non-additive-measure trap.

**Single-supplier alert** — fires when one supplier holds more than **5%** of a
month's spend. The threshold is a choice, not a derivation: at 10k suppliers an
even split gives each 0.01%, so 5% means one supplier is carrying five hundred
times its share and is worth a human look. Tune it in
`monitoring.SINGLE_SUPPLIER_SHARE_THRESHOLD`.

## Status

| | owner | state |
|---|---|---|
| repo, config, job | person 1 | done |
| bronze | person 1 | done |
| silver | person 2 | done |
| gold | person 3 | not started |
| monitoring + alert | person 1 | done |

## Presentation

*(link goes here)*
