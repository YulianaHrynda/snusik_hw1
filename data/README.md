# Lab 4 — A medallion lakehouse on TPC-H

**Lecture:** L4 — Lakehouse & data modelling
**Time:** ~60 minutes to run and read, longer if you follow the *try it* prompts
**Prerequisite:** L3 — Delta, and specifically `MERGE`

A walkthrough, not an assignment. **Nothing here is graded and there is nothing to submit.**
Run `L4_lakehouse.py` top to bottom — it needs no editing, and writes to a schema named after
your login — then go back and break things.

The lecture's group assignment is briefed separately.

## The dataset

`samples.tpch` — the TPC-H decision-support benchmark, present in every Databricks workspace.
Eight tables modelling a wholesale supplier:

```
region  <- nation <- customer <- orders <- lineitem -> partsupp -> part
                                                                -> supplier
```

No download, no credentials, identical for everyone. It is also completely clean, which would
make a data-quality layer pointless — so the notebook damages it first, on named columns and a
fixed set of rows, the same way every run.

## What the notebook walks through

Each section builds a layer and then shows what that layer bought you. The second half is the
point; the building is just how you get there.

**1. Explore the source — and find the grain.** "One row per *what*?" `orders` is one row per
order, `lineitem` one row per line. That single fact decides everything downstream: why silver
must keep both grains, and why `fct_orders` may aggregate one into the other.

**2. Bronze — capture.** Everything the source had, plus `_ingested_at` and `_source`. Nothing
parsed, nothing filtered, nothing fixed. *Bought you:* a table that can say what arrived, from
where, and when — the question nobody can answer once you have cleaned data on the way in.

**3. Silver — enforce, and quarantine what fails.** Type, deduplicate, apply rules, and route
failures to a `quarantine_*` table with the reason attached, rather than dropping them.
*Bought you:* trustworthy tables at the original grain, plus a written record of what was
wrong with the feed.

There is a sub-section on why `split_valid` wraps every rule in `coalesce(..., false)`: a rule
that returns NULL passes neither `filter(cond)` nor `filter(~cond)`, so the row lands in
*neither* table and disappears silently. The notebook shows the row counts side by side.
Lecture 11 makes you find this one yourself; here you just get to see it.

**4. Gold — the star schema.** `dim_date`, `dim_part`, `fct_orders` at order grain.
*Bought you:* the same business question in a two-table join instead of a five-table one, in
business vocabulary rather than source-system vocabulary. There is also a worked demonstration
of an **additive vs non-additive measure** — why summing an average discount produces a number
with no meaning, and how far off it actually is.

**5. SCD Type 2 on `dim_customer`.** The full two-pass `MERGE`: close the current row, then
insert the new version with a fresh surrogate key. Both lecture scenarios are exercised — a
customer who moves *and* a customer who unsubscribes, because a status change is a change like
any other. *Bought you:* the point-in-time query, shown next to the Type 1 dimension where the
same question cannot be expressed at all.

The interval convention matters and is stated in the notebook:
`[effective_date, expiry_date)` — effective inclusive, expiry **exclusive**, NULL for the
current row. Getting the boundary wrong is the most common SCD Type 2 bug there is.

**6. What the layers cost, and what they bought.** The quality summary, and the same query run
against the raw source and against gold.

## Where to go from here

The notebook ends with these; they are the interesting part.

- Build `fct_lineitem` at line grain and join it to `dim_part`. Which questions does it answer
  that `fct_orders` cannot?
- `fct_orders` joins `dim_customer` on `customer_id`. It should join on `sk`, so each order
  resolves to the customer *as they were at the time*. What breaks, and what would you change?
- Apply a second batch of changes on a later date. Do you get three versions for a customer
  who moved twice?
- Nearly every downstream query filters on `is_current`. Delta has no secondary indexes — look
  at liquid clustering, `ZORDER` and partitioning, and decide whether any of them is worth it
  for a two-valued column.
- Snowflake `dim_customer` by splitting nation and region back out. Measure whether the storage
  saving justifies the extra join.

## Also in this folder — `L4b_why_quality_rules.py`

**Time:** ~30 minutes · self-contained, does not need the walkthrough above to have been run

The walkthrough builds a `quarantine_*` table for every silver table. If you have never
watched a business number go wrong, that looks like paperwork. This notebook makes the case.

One orders feed, one number the business publishes (1997 revenue by market segment), and 155
broken rows out of ~228,000 — a 1000× price error, missing customer ids, a negative amount,
some duplicates. Then four pipelines over the same input:

| | ignore | fail the job | drop the rows | quarantine |
|---|---|---|---|---|
| publishes a correct number | ✗ | — | ✓ | ✓ |
| publishes anything at all | ✓ | ✗ | ✓ | ✓ |
| can say what was rejected | ✗ | ✗ | ✗ | **✓** |
| can recover the rejected rows | — | — | ✗ | **✓** |
| the books balance | ✓ | ✓ | ✗ | **✓** |

Two results are worth arriving at yourself:

- **A 0.07% defect rate is not a 0.07% error.** The damage lands almost entirely on `sum`,
  which is the statistic that gets published. `count` and `median` barely move.
- **Drop and quarantine publish the same number today.** Dropping is not wrong, it is
  *unaccountable* — and it permanently deletes rows that were real orders with real revenue,
  recorded badly. The notebook repairs them out of quarantine and lands back on the exact
  right answer.

What it deliberately does *not* cover — thresholds, rule ownership, alerting on the failure
rate, re-drive as a routine, monitoring quality over time — is Lecture 11.

## Downstream

Lab 8 reads the gold tables this notebook writes, from the same personal schema. If you want
to do Lab 8, run this one first.
