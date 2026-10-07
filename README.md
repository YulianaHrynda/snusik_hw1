# TPC-H Lakehouse: Procurement

This is our group project for the Big Data course (Group Assignment 1).

We took the TPC-H sample data that comes with every Databricks workspace and built a
small lakehouse on it with three layers: bronze, silver and gold. Then we used it to
answer questions for the **Procurement** team. They want to know what we pay our
suppliers, whether we depend too much on one of them, and which suppliers are worth
keeping.

The task description is in [`group_assignment_1.pdf`](group_assignment_1.pdf).

## Team

| Who | What they did |
|---|---|
| Person 1, Ostap Mnykh | repo setup, config, bronze layer, Databricks job, monitoring |
| Person 2 | silver layer, data checks, ER diagram |
| Person 3, Yulian Zaiats | gold tables, questions 1 and 2 |
| Person 4, Yuliana Hrynda | questions 3 and 4, dashboard |

## What is in the repo

```
conf/config.yaml        all table and schema names live here
src/tpch_lakehouse/     the pipeline code
  bronze.py             copies the raw data
  silver.py             cleans and checks the data
  gold.py               builds tables for the questions
  monitoring.py         tracks numbers over time and raises an alert
notebooks/              notebooks with the answers and charts
dashboards/             the Databricks dashboard
docs/silver_er.svg      ER diagram of the silver tables
resources/              the Databricks job and dashboard setup
tests/                  tests that run on your laptop, no cluster needed
data/                   lecture examples, just for reference
```

## How to run it

On your computer, you need [uv](https://docs.astral.sh/uv/) and Java (for the tests):

```bash
uv sync
uv run pytest
```

On Databricks, you need the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/)
and a login:

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com
databricks bundle deploy -t dev
databricks bundle run tpch_medallion -t dev
```

This creates a job that runs bronze → silver → gold → monitoring, plus a dashboard.
It works on the free Databricks edition.

If you share a workspace with your team, someone may already own the default schemas.
In that case, use your own name in the schema prefix:

```bash
databricks bundle deploy -t dev --var schema_prefix=tpch_procurement_<yourname>
databricks bundle run tpch_medallion -t dev --var schema_prefix=tpch_procurement_<yourname>
```

After the job finishes, open `notebooks/show_gold_questions.py` (questions 1–2) or
`notebooks/show_gold_questions_q3_q4.py` (questions 3–4) in Databricks and click
**Run all**.

## No hardcoded names

The code never writes table or schema names directly. They all come from
`conf/config.yaml`, and you can change them with command-line flags or `TPCH_*`
environment variables. This way the same code can run in another workspace, or on
pre-production data. A test checks that nobody added a fixed name by mistake.

## The three layers

**Bronze** is an exact copy of the source tables. We change nothing. We only add two
columns: when the data arrived (`_ingested_at`) and where it came from (`_source`).

**Silver** has the same 8 tables, cleaned and checked. Rows that break a rule are not
deleted. They go to a `quarantine_<table>` table with the reason written next to them,
so nothing gets lost and every row can be explained.

**Gold** has tables built for the Procurement questions:

| Table | One row per | Used for |
|---|---|---|
| `supplier_inventory` | supplier | Q1 |
| `brand32_sourcing` | Brand#32 part | Q2 |
| `part_sourcing` | part | Q3 |
| `supplier_complaints` | supplier | Q4 |
| `supplier_spend_monthly` | supplier and month | Q4, dashboard |
| `part_supplier_activity` | part and supplier pair | helper for Q2 and Q3 |
| `monitor_*` | month | monitoring |

## How we check the data

All the checks are in `silver.py`, and `notebooks/show_silver_validation.py` shows
the results.

- **Every order line must match a real supply deal.** A part and a supplier are linked
  by two columns together, `(partkey, suppkey)`. We do a `LEFT ANTI JOIN` against
  `partsupp` on both columns. Any line that finds no match goes to quarantine.
  Checking only one of the columns would not be enough.
- **Every supplier must have a real nation, and every nation a real region.** This is
  the same kind of join.
- **Supply cost must be above 0 and not higher than the part's retail price.**
- If a check can't give an answer (for example, because of a missing value), we count
  it as failed, so no row can quietly disappear.

Databricks lets us declare primary and foreign keys, but it doesn't actually enforce
them. We still declare them for documentation. The joins above are what really
protect the data.

## How we count things

- **Spend** is what we pay a supplier: `supply cost × quantity`. We do not use the
  price the customer paid, because that is our income, not our cost.
- **Inventory value** is `available quantity × supply cost`, added up per supplier.
- **A single-sourced part** is a part we only ever bought from one supplier, even
  though other suppliers also offer it.
- **A supplier with complaints** has `Customer ... Complaints` in its comment text.
  TPC-H writes complaints in exactly this form.
- **Average supply cost by region** is total spend divided by total quantity. A plain
  average of prices would ignore how much we actually bought.
- **The alert** goes off if one supplier gets more than 5% of a month's spend.

## Results

These come from a full run on 2026-10-07. The data has 50,000 suppliers, 1 million
parts and about 30 million order lines.

**Data checks.** Every table adds up: bronze = silver + quarantine. We found one real
problem in the source data. 3,479 supply deals have a supply cost higher than the
retail price. For example, part 1 from supplier 12502 costs 993.49, but the part sells
for 901.00. These deals went to quarantine, and so did the 26,433 order lines that
used them.

**Q1. Which 10 suppliers have the biggest inventory value?** Together they hold
0.0276% of all inventory value. That is very close to an even split, so stock is
spread out and no supplier stands out. The biggest is Supplier#000037953 from
Mozambique, with 282.1 million.

**Q2. How much do supply costs differ for Brand#32 parts, and do we buy from the
cheapest supplier?** For the same part, the cheapest and the most expensive supplier
differ by 598.97 per unit on average, and by up to 996.02. Only 24.97% of what we
bought came from the cheapest supplier. That is about what you would get by picking
one of four suppliers at random, so price is not driving our choice.

**Q3. Which parts do we buy from only one supplier when others are available?** None.
Every part was bought from several suppliers. There are 10 parts with just one
supplier, but only because their other supply deals were quarantined in silver. So
there is no risk here, and nothing is concentrated in one region or brand.

**Q4. Which suppliers have complaints, and how much do we spend with them?** 26 of
50,000 suppliers have complaints. They get 0.053% of our spend, which is about their
fair share. So complaints don't change how much we buy from a supplier. This share
stays steady from month to month.

**Monitoring.** We have 80 months of data, from January 1992 to August 1998. No single
supplier ever got more than 0.033% of a month's spend, so the 5% alert never went off.
The supply cost per unit is about 500 in every region.

## Presentation

*(link goes here)*
