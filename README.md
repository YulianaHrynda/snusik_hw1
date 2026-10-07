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
  gold.py                   procurement marts                 (persons 3, 4)
  monitoring.py             metrics over time + alert         (person 1)
notebooks/run_bronze.py     run bronze from a Databricks Git folder
notebooks/show_silver_validation.py   silver checks, for the demo
notebooks/show_gold_questions.py     Q1/Q2 answers and charts (person 3)
notebooks/show_gold_questions_q3_q4.py  Q3/Q4 answers and charts (person 4)
dashboards/procurement.lvdash.json   Databricks dashboard, all questions + monitoring (person 4)
docs/silver_er.svg          ER diagram of the silver tables
databricks.yml              asset bundle
resources/                  the job: bronze -> silver -> gold -> monitoring, and the dashboard
tests/                      config, silver and gold tests, runnable without a cluster
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

The deploy also publishes the **Procurement dashboard**. It runs on the SQL
warehouse named `Serverless Starter Warehouse`. On a workspace without one, pass
`--var warehouse_id=<id>` to `validate` and `deploy`. Its queries name gold tables
without a catalog or schema. The bundle sets `dataset_catalog` and
`dataset_schema` to the target's gold schema, so the same JSON serves dev and
preprod. Run the job at least once before opening the dashboard.

In a shared workspace the schemas belong to whoever created them first. To run
your own copy, give the deploy and the run a personal prefix:

```bash
databricks bundle deploy -t dev --var schema_prefix=tpch_procurement_<you>
databricks bundle run tpch_medallion -t dev --var schema_prefix=tpch_procurement_<you>
```

The job runs on serverless compute, so it works on Databricks Free Edition.

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

### Gold tables and Questions 1–2

The `gold` entry point builds six tables in the configured gold schema.
Each run replaces these aggregates, even if the ingestion write mode is `append`,
so reruns do not duplicate business totals. Bronze and silver are only read.

| table | grain | use |
|---|---|---|
| `supplier_inventory` | one supplier | stock value, deterministic rank, share of all inventory, supplier comments and geography |
| `supplier_spend_monthly` | one active supplier per calendar month | spend, quantity, line/order counts, supplier comments and geography |
| `part_supplier_activity` | one available part–supplier pair | listed cost and stock, order activity, brand and supplier geography; all brands, including unused alternatives |
| `brand32_sourcing` | one Brand#32 part with supplier offers | minimum/maximum cost, gaps, cheapest supplier keys, quantities and sourcing classification |
| `part_sourcing` | one part with at least one supply agreement | suppliers available vs ordered from, sourcing status, sole supplier and its geography, spend |
| `supplier_complaints` | one supplier | complaint flag from `s_comment`, all-time spend and share of total spend, geography |

**Q1:** read the first ten ranks from `supplier_inventory`; sum their inventory
values and divide by the inventory value across **all** suppliers. Suppliers with
no supply agreements remain in the table with zero inventory. Equal values are
ordered by supplier key, so the result contains at most ten suppliers consistently.
Inventory is aggregated before considering any orders; repeated orders cannot
multiply available stock.

**Q2:** compare every listed supplier for each Brand#32 part, then compare their
offers with actual line-item usage. All suppliers tied for the minimum price count
as cheapest. The table distinguishes `CHEAPEST_ONLY`, `MIXED`, `OTHER_ONLY` and
`NOT_ORDERED`. Unordered parts retain their price comparison, but have NULL usage
flags and NULL quantity share rather than being counted as poor sourcing choices.
This comparison covers all available order dates.

After the pipeline finishes, open `notebooks/show_gold_questions.py` in the
Databricks Git folder, attach a cluster, and run all. Match its `load_config`
overrides to the job's catalog/environment/schema prefix. The notebook only reads
gold and uses the cluster's Matplotlib to render:

- Q1: the top-ten inventory bar chart and their combined share.
- Q2: cheapest/most-expensive costs for the 15 largest gaps, sourcing categories,
  and ordered quantities from cheapest versus other suppliers.

The notebook also displays the complete comparison table and an overall summary.
It collects only the small chart inputs on the driver.

Monitoring reads silver directly. Gold uses the same spend and period
definitions, so the two agree.

### Questions 3–4

**Q3:** `part_sourcing` rolls `part_supplier_activity` up to one row per part. A
part is single-sourced (`SINGLE_WITH_ALTERNATIVES`) when exactly one supplier was
ordered from and `partsupp` lists at least one other. Parts with only one listed
supplier (`SINGLE_NO_ALTERNATIVE`) are counted separately, because there was no
choice. Parts never ordered (`NOT_ORDERED`) are not single-sourced. The sole
supplier's key, nation and region are filled only when there is exactly one, so
the risk can be grouped by geography, brand or supplier. A grouping counts as a
concentration only when its share of single-sourced parts is clearly above its
share of all spend. The notebook shows the two shares side by side.

**Q4:** a supplier has a complaint when `s_comment LIKE '%Customer%Complaints%'`.
That is how the TPC-H generator plants complaints, and the spec's own Query 16
filters on the same pattern. The match is case-sensitive. The notebook profiles
looser alternatives (any case, the word on its own) and shows what they would
add. A NULL comment counts as no complaint, so the two groups always add up to
all spend. The answer is the complaint suppliers' spend divided by total spend.
The notebook compares it with their share of the supplier count, and plots the
monthly share from `supplier_spend_monthly`.

Open `notebooks/show_gold_questions_q3_q4.py` the same way as the Q1/Q2
notebook. It only reads gold, plus silver `supplier` for the pattern profiling.

### Dashboard

`dashboards/procurement.lvdash.json` has four pages: an overview with headline
counters, Q1–Q2, Q3–Q4, and monitoring (top-10 spend concentration, supply cost
by region, largest single-supplier share against the 5% alert line). Every
dataset query reads gold only. `tests/test_gold.py` runs each query on a local
Spark and checks that every widget's fields exist. Edit the dashboard in the UI
if you like, then export it back over this file so git stays the source of truth.

## Metric definitions

Every headline number is defined once here, and every query in the repo uses
that definition. If a gold answer and a monitoring chart disagree, one of them
stopped following this section.

**Inventory value** — `sum(ps_availqty * ps_supplycost)` per supplier, a snapshot
of available stock. It has no order-date dimension. Q1's combined top-ten share
is the sum for the ten ranked suppliers divided by the total for all suppliers.

**Brand#32 supply-cost gap** — maximum minus minimum listed supply cost for the
same part. The relative gap is `(maximum - minimum) / minimum`; multiply by 100
to display a percentage. Compare prices within a part, not across different parts.

**Cheapest-supplier usage** — `orders_from_cheapest` means at least one line uses
a supplier at the minimum listed cost; `orders_only_from_cheapest` means every
line does. All tied suppliers qualify. Quantity share is units sourced from
cheapest suppliers divided by all ordered units for that part. Overall quantity
share uses summed quantities, not the average of per-part shares. Shares are
stored as fractions; unordered parts have NULL shares.

**Spend** — what we pay a supplier:

```
spend = ps_supplycost * l_quantity
```

Not `l_extendedprice`. That is what a customer pays *us*, which is a Finance
number; Procurement is asked what the company pays out. A line item resolves to
its supply agreement on **both** keys together, `(l_partkey, l_suppkey)` —
joining on the part alone multiplies spend by the number of suppliers offering
that part.

Supply costs in `partsupp` have no historical effective dates. Spend therefore
uses the listed cost for each ordered quantity; it does not reconstruct supplier
invoices or historical prices. Q2 price gaps describe listed alternatives, without
assuming identical capacity, service, or delivery terms.

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

**Single-sourced part** — a part ordered from exactly one supplier, over all
order dates, although `partsupp` lists more than one supplier for it.

**Complaint supplier** — `s_comment LIKE '%Customer%Complaints%'`
(`gold.COMPLAINT_PATTERN`). Its spend share is its spend divided by total spend
across all suppliers and all order dates.

**Single-supplier alert** — fires when one supplier holds more than **5%** of a
month's spend. The threshold is a choice, not a derivation: at 10k suppliers an
even split gives each 0.01%, so 5% means one supplier is carrying five hundred
times its share and is worth a human look. Tune it in
`monitoring.SINGLE_SUPPLIER_SHARE_THRESHOLD`.

## Results

From a full run on `samples.tpch` (50,000 suppliers, 1,000,000 parts, 30M line
items) on 2026-10-07, in the `tpch_procurement_hrynda_*` schemas. Money is in the
dataset's currency units.

### Validation

Every layer reconciles: bronze = silver + quarantine for all eight tables.

| table | bronze | silver | quarantined | rule |
|---|---:|---:|---:|---|
| partsupp | 4,000,000 | 3,996,521 | 3,479 | `supplycost_within_retail` |
| lineitem | 29,999,795 | 29,973,362 | 26,433 | `fk_lineitem_partsupp` |
| other six tables | | | 0 | |

The source itself contains 3,479 supply agreements whose supply cost is above
the part's retail price. For example, part 1 from supplier 12502 costs 993.49,
while the part retails at 901.00. The profile's rule rejects them. The 26,433
line items bought under those agreements then fail the two-column foreign key,
because their `(partkey, suppkey)` pair is no longer in silver. All answers
below exclude both.

### Q1. Ten largest inventories

The top ten suppliers hold **2,756,133,313.28** of **9,996,460,036,124.43**
inventory value, which is **0.0276%**. An even split across 50,000 suppliers would
give ten of them 0.02%, so inventory is spread almost evenly. The largest
inventory is Supplier#000037953 (Mozambique), at 282.1M.

### Q2. Brand#32 supply-cost spread

- **Spread:** across 39,792 Brand#32 parts with about four offers each, the
  cheapest and most expensive supplier differ by **598.97** per unit on average,
  and by at most **996.02**.
- **Do we order from the cheapest supplier?** Mostly not. 39,778 parts were
  ordered from a cheapest supplier at least once, 14 never were, and no part was
  ordered only from the cheapest.
- **By quantity:** **24.97%** of units were bought at the cheapest price. That is
  what choosing one of four suppliers at random would give, so price does not
  drive which supplier is used.

### Q3. Single-sourced parts

**None.** No part was ordered from a single supplier while `partsupp` listed
others.

- 999,989 parts were ordered from more than one supplier.
- Each part has at least 3 order lines, and 30 at the median, spread across its
  suppliers.
- 10 parts were ordered from one supplier, but no other supplier offers them.
  That is only because their other agreements were quarantined for supply cost
  above retail.

So there is no single-sourcing risk, and no concentration to report by region,
brand or supplier.

### Q4. Suppliers with complaints

- **Who:** **26** of 50,000 suppliers (**0.052%**) match
  `'%Customer%Complaints%'`. Looser patterns find the same 26 (the word
  "Complaints" alone, or "complain" in any case).
- **By region:** Europe 9, America 7, Asia 6, Middle East 3, Africa 1.
- **Spend:** **202,525,529.79** of **382,304,884,533.07**, which is **0.0530%** of
  all spend. That is 1.02 times their share of the supplier count, so we buy
  from them about as much as from anyone else; complaints do not reduce spend.
- **Over time:** the monthly share stays between 0.044% and 0.067%.

### Monitoring

- **Coverage:** 80 months, from 1992-01 to 1998-08.
- **Concentration:** the top ten suppliers hold between 0.065% and 0.298% of a
  month's spend.
- **Alert:** the largest single-supplier share in any month is 0.033%, far below
  the 5% threshold, so it never fires.
- **Supply cost per unit:** between 499.83 and 500.39 in every region.

## Status

| | owner | state |
|---|---|---|
| repo, config, job | person 1 | done |
| bronze | person 1 | done |
| silver | person 2 | done |
| gold + Q1/Q2 code and visualisations | person 3 | implemented |
| monitoring + alert | person 1 | done |
| gold Q3/Q4, visualisations, dashboard | person 4 | done |

## Presentation

*(link goes here)*
