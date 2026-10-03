# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 4b — Why quarantine at all?
# MAGIC
# MAGIC The main L4 walkthrough built a `quarantine_*` table for every silver table, and if
# MAGIC you have never watched a business number go wrong, that probably looked like paperwork.
# MAGIC
# MAGIC This notebook makes the case. One orders feed, one number the business reports, and
# MAGIC four different things you could do about the handful of rows that arrive broken.
# MAGIC
# MAGIC Self-contained — it does not need the L4 walkthrough to have been run.
# MAGIC
# MAGIC 1. A number the business cares about
# MAGIC 2. Monday: the feed changed
# MAGIC 3. The blast radius
# MAGIC 4. Four pipelines, one dataset
# MAGIC 5. Closing the books
# MAGIC 6. What it cost

# COMMAND ----------

import re

from pyspark.sql import functions as F

user = spark.sql("SELECT current_user()").first()[0]
schema = "lakehouse_" + re.sub(r"\W+", "_", user.split("@")[0]).lower()

spark.sql(f"CREATE SCHEMA IF NOT EXISTS workspace.{schema}")
spark.sql(f"USE workspace.{schema}")
print(f"Working in workspace.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. A number the business cares about
# MAGIC
# MAGIC You run the orders feed for a wholesaler. Every Monday, finance publishes **revenue by
# MAGIC market segment** for the year to date, and that number goes in front of people who make
# MAGIC decisions with it.
# MAGIC
# MAGIC Here is the 1997 feed, clean, straight from the source system.

# COMMAND ----------

clean = (
    spark.table("samples.tpch.orders")
    .filter("year(o_orderdate) = 1997")
    .select(
        F.col("o_orderkey").cast("bigint").alias("order_id"),
        F.col("o_custkey").cast("bigint").alias("customer_id"),
        F.col("o_totalprice").cast("decimal(20,2)").alias("total_price"),
        F.col("o_orderdate").cast("date").alias("order_date"),
    )
)
clean.write.mode("overwrite").saveAsTable("dq_orders_clean")
clean = spark.table("dq_orders_clean")

print(f"dq_orders_clean: {clean.count():,} orders")
display(clean.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC The market segment lives on the customer, not the order, so the report is a join. This
# MAGIC is the pipeline, all of it:

# COMMAND ----------

customers = spark.table("samples.tpch.customer")


def revenue_by_segment(orders):
    """The weekly report: revenue by market segment."""
    return (
        orders.alias("o")
        .join(customers.alias("c"), F.col("o.customer_id") == F.col("c.c_custkey"))
        .groupBy(F.trim(F.col("c.c_mktsegment")).alias("market_segment"))
        .agg(F.sum("o.total_price").alias("revenue"))
        .orderBy("market_segment")
    )


def total_revenue(orders):
    return float(revenue_by_segment(orders).agg(F.sum("revenue")).first()[0])


truth = total_revenue(clean)
print(f"1997 revenue, from the clean feed: {truth:,.0f}")
display(revenue_by_segment(clean))

# COMMAND ----------

# MAGIC %md
# MAGIC Hold on to that number. It is the only time in this notebook — and roughly the only
# MAGIC time in your career — that you will know what the right answer actually was.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Monday: the feed changed
# MAGIC
# MAGIC Nobody told you. An upstream system was upgraded over the weekend and the export is
# MAGIC subtly different. Four things are wrong with it, and all four are things that really
# MAGIC happen:
# MAGIC
# MAGIC | what went wrong | rows | how it happens in real life |
# MAGIC |---|---|---|
# MAGIC | price is 1000× too big | 40 | a currency stored in minor units, or a decimal shift |
# MAGIC | `customer_id` is missing | 40 | an optional field in a new API version |
# MAGIC | price is negative | 15 | a refund booked as a sale |
# MAGIC | the whole order is duplicated | 60 | a retried batch that was not idempotent |
# MAGIC
# MAGIC That is **155 orders out of ~228,000.** Two defects push revenue up and two push it
# MAGIC down, which is exactly why "we would have noticed" is not a plan.

# COMMAND ----------

N_UNIT, N_NULL, N_NEG, N_DUP = 40, 40, 15, 60

keys = [r[0] for r in clean.select("order_id").orderBy("order_id")
        .limit(N_UNIT + N_NULL + N_NEG + N_DUP).collect()]
unit_keys = keys[0:N_UNIT]
null_keys = keys[N_UNIT:N_UNIT + N_NULL]
neg_keys = keys[N_UNIT + N_NULL:N_UNIT + N_NULL + N_NEG]
dup_keys = keys[N_UNIT + N_NULL + N_NEG:]

damaged = (
    clean
    .withColumn("total_price",
                F.when(F.col("order_id").isin(unit_keys),
                       (F.col("total_price") * 1000).cast("decimal(20,2)"))
                 .otherwise(F.col("total_price")))
    .withColumn("total_price",
                F.when(F.col("order_id").isin(neg_keys), -F.abs(F.col("total_price")))
                 .otherwise(F.col("total_price")))
    .withColumn("customer_id",
                F.when(F.col("order_id").isin(null_keys), F.lit(None))
                 .otherwise(F.col("customer_id")))
)

raw = damaged.unionByName(damaged.filter(F.col("order_id").isin(dup_keys)))
raw.write.mode("overwrite").saveAsTable("dq_orders_raw")
raw = spark.table("dq_orders_raw")

print(f"dq_orders_clean: {clean.count():,} rows")
print(f"dq_orders_raw:   {raw.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC Nothing about this fails. The file arrived, the schema matched, the job ran green, the
# MAGIC row count moved by 0.03% and no alert fired anywhere.

# COMMAND ----------

display(
    raw.filter(F.col("order_id").isin(unit_keys[:3] + null_keys[:3] + neg_keys[:3]))
    .orderBy("order_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The blast radius
# MAGIC
# MAGIC Publish the report from the feed as it arrived.

# COMMAND ----------

dirty = total_revenue(raw)
defect_rate = 100 * (N_UNIT + N_NULL + N_NEG + N_DUP) / clean.count()
error_rate = 100 * (dirty - truth) / truth

print(f"correct revenue    {truth:>20,.0f}")
print(f"published revenue  {dirty:>20,.0f}")
print()
print(f"rows affected      {defect_rate:>19.4f}% of the feed")
print(f"error in the number{error_rate:>19.2f}%")
print()
print(f"the error is {error_rate / defect_rate:,.0f}x larger than the defect rate")

# COMMAND ----------

display(
    revenue_by_segment(clean).withColumnRenamed("revenue", "correct").alias("a")
    .join(revenue_by_segment(raw).withColumnRenamed("revenue", "published").alias("b"),
          "market_segment")
    .withColumn("error_pct",
                F.round(100 * (F.col("published") - F.col("correct")) / F.col("correct"), 2))
    .orderBy("market_segment")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why so much damage from so few rows
# MAGIC
# MAGIC Because the statistic you report is the one least able to absorb a bad value. Watch
# MAGIC what the same 155 rows do to four different summaries of the same column.

# COMMAND ----------

def summarise(df, label):
    return df.select(
        F.lit(label).alias("feed"),
        F.count("*").alias("orders"),
        F.sum("total_price").alias("sum"),
        F.avg("total_price").cast("decimal(20,2)").alias("mean"),
        F.expr("percentile_approx(total_price, 0.5)").cast("decimal(20,2)").alias("median"),
    )


display(summarise(clean, "clean").unionByName(summarise(raw, "as received")))

# COMMAND ----------

# MAGIC %md
# MAGIC `count` barely moves. The `median` barely moves. The **`sum` moves enormously** — and
# MAGIC the sum is what finance publishes.
# MAGIC
# MAGIC That is the whole problem in one line: **a 0.07% defect rate is not a 0.07% error.**
# MAGIC Aggregation concentrates the damage instead of diluting it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Four pipelines, one dataset
# MAGIC
# MAGIC So you need to do *something* about the bad rows. There are four instincts, and it is
# MAGIC worth running all four rather than arguing about them.
# MAGIC
# MAGIC First, write down what "good" means. Three rules — no more than the business could
# MAGIC actually tell you:

# COMMAND ----------

RULES = {
    "has_customer":    "customer_id IS NOT NULL",
    "price_positive":  "total_price > 0",
    "price_plausible": "total_price < 600000",   # no single order has ever come close
}
RULES_SQL = " AND ".join(f"({c})" for c in RULES.values())
print(RULES_SQL)

# COMMAND ----------

def split_valid(df, rules):
    """Same split as the L4 walkthrough: passing rows, failing rows, reason attached."""
    conds = [F.coalesce(F.expr(c), F.lit(False)).alias(n) for n, c in rules.items()]
    tagged = df.select("*", *conds)

    all_pass = F.lit(True)
    for name in rules:
        all_pass = all_pass & F.col(name)
    failed = F.concat_ws(",", *[F.when(~F.col(n), F.lit(n)) for n in rules])

    good = tagged.filter(all_pass).drop(*rules.keys())
    bad = (tagged.filter(~all_pass)
           .withColumn("_failed_rules", failed)
           .withColumn("_quarantined_at", F.current_timestamp())
           .drop(*rules.keys()))
    return good, bad

# COMMAND ----------

# MAGIC %md
# MAGIC ### Instinct 1 — ignore it
# MAGIC
# MAGIC No rules. The data is what the source sent; that is the source's problem.

# COMMAND ----------

gold_ignore = raw
gold_ignore.write.mode("overwrite").saveAsTable("dq_gold_ignore")
print(f"delivered {spark.table('dq_gold_ignore').count():,} orders — job green")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Instinct 2 — fail the job
# MAGIC
# MAGIC If the data is wrong, stop. Refuse to publish anything until somebody fixes it. This is
# MAGIC what a `CHECK` constraint or a strict expectation does.

# COMMAND ----------

violations = raw.filter(~F.coalesce(F.expr(RULES_SQL), F.lit(False))).count()

fail_status, gold_fail_rows = "succeeded", raw.count()
try:
    if violations:
        raise ValueError(f"{violations} rows violate the quality rules — refusing to load")
    raw.write.mode("overwrite").saveAsTable("dq_gold_fail")
except ValueError as e:
    fail_status, gold_fail_rows = "FAILED", 0
    print(f"pipeline aborted: {e}")

print(f"\ndelivered {gold_fail_rows:,} orders")
print(f"{raw.count() - violations:,} perfectly good orders are now also unavailable")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Instinct 3 — drop the bad rows
# MAGIC
# MAGIC Deduplicate, filter out anything that fails a rule, carry on. One line, and it is by
# MAGIC far the most common thing people actually do.

# COMMAND ----------

gold_drop = raw.dropDuplicates(["order_id"]).filter(RULES_SQL)
gold_drop.write.mode("overwrite").saveAsTable("dq_gold_drop")
print(f"delivered {spark.table('dq_gold_drop').count():,} orders — job green")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Instinct 4 — quarantine
# MAGIC
# MAGIC Same rules, but the failures are written to a second table with the reason attached
# MAGIC instead of being deleted.

# COMMAND ----------

good, bad = split_valid(raw.dropDuplicates(["order_id"]), RULES)
good.write.mode("overwrite").saveAsTable("dq_gold_quarantine")
bad.write.mode("overwrite").saveAsTable("dq_quarantine")

print(f"delivered   {spark.table('dq_gold_quarantine').count():,} orders — job green")
print(f"quarantined {spark.table('dq_quarantine').count():,} orders")
display(spark.table("dq_quarantine").groupBy("_failed_rules").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### The scorecard
# MAGIC
# MAGIC Four pipelines, same input, same rules. Everything below is measured, not asserted.

# COMMAND ----------

raw_rows = raw.count()
dropped_rows = spark.table("dq_gold_drop").count()
quarantined_rows = spark.table("dq_quarantine").count()
q_gold_rows = spark.table("dq_gold_quarantine").count()

scorecard = [
    ("1. ignore",     total_revenue(spark.table("dq_gold_ignore")),
     raw_rows, raw_rows - raw_rows, "green", "no", "n/a"),
    ("2. fail",       0.0,
     0, raw_rows, "RED", "no", "n/a"),
    ("3. drop",       total_revenue(spark.table("dq_gold_drop")),
     dropped_rows, raw_rows - dropped_rows, "green", "no", "no"),
    ("4. quarantine", total_revenue(spark.table("dq_gold_quarantine")),
     q_gold_rows, raw_rows - q_gold_rows, "green", "yes", "yes"),
]

rows = [
    (name, rev, round(100 * (rev - truth) / truth, 2), gold_n, lost, status, explain, recover)
    for name, rev, gold_n, lost, status, explain, recover in scorecard
]

display(spark.createDataFrame(rows, """
    approach string, published_revenue double, error_pct double,
    orders_delivered long, orders_not_delivered long,
    job_status string, `knows_which_rows_were_bad` string, `can_recover_them` string
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Read that table slowly, because the interesting result is **not** the one people
# MAGIC expect.
# MAGIC
# MAGIC * **Ignore** is the only one with a badly wrong number. It is also the only one where
# MAGIC   nobody will ever find out.
# MAGIC * **Fail** delivers nothing at all. 228,000 good orders are held hostage by 155 bad
# MAGIC   ones, and somebody is now awake at 3am to fix a report that was not urgent.
# MAGIC * **Drop and quarantine produce the *same* number today.** Dropping is not wrong. It
# MAGIC   is *unaccountable*.
# MAGIC
# MAGIC The last two columns are where they separate, and they only start to matter on the day
# MAGIC somebody asks a question.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Closing the books
# MAGIC
# MAGIC Here is the question. It is Tuesday, finance has the report, and someone emails:
# MAGIC
# MAGIC > *"This is down on last year. Are we sure nothing is missing?"*
# MAGIC
# MAGIC Answering means closing one equation:
# MAGIC
# MAGIC ```
# MAGIC rows received  =  rows delivered  +  rows rejected  +  duplicates removed
# MAGIC ```

# COMMAND ----------

dupes_removed = raw_rows - raw.dropDuplicates(["order_id"]).count()

print(f"rows received: {raw_rows:,}\n")

print(f"1. ignore      {raw_rows:,} = {raw_rows:,} + 0 + 0")
print("               balances — but only because nothing was ever rejected\n")

print(f"2. fail        {raw_rows:,} = 0 + {raw_rows:,} + 0")
print("               balances, and delivers nothing\n")

print(f"3. drop        {raw_rows:,} = {dropped_rows:,} + ? + ?")
print(f"               {raw_rows - dropped_rows:,} rows are unaccounted for. Which ones? Why?")
print("               The information was destroyed by the filter.\n")

print(f"4. quarantine  {raw_rows:,} = {q_gold_rows:,} + {quarantined_rows:,} + {dupes_removed:,}"
      f"  -> {q_gold_rows + quarantined_rows + dupes_removed == raw_rows}")
print("               balances, itemised, and every rejected row is still readable")

# COMMAND ----------

# MAGIC %md
# MAGIC Notice that **ignore balances too.** A closing equation is necessary, not sufficient —
# MAGIC it proves nothing was lost, not that anything was right. Quarantine is the only column
# MAGIC that gets to claim both.

# COMMAND ----------

# MAGIC %md
# MAGIC ### And the rows are still there, so they can be fixed
# MAGIC
# MAGIC This is the part that dropping cannot do at any price. Those rejected rows are not
# MAGIC garbage —
# MAGIC they are **real orders with real revenue**, recorded badly:
# MAGIC
# MAGIC * the 1000× rows just need dividing
# MAGIC * the negative rows are refunds with the sign flipped
# MAGIC * the missing customer ids can be looked up in the source system
# MAGIC
# MAGIC Dropping them deleted real money and left no trace. Quarantining them means Tuesday's
# MAGIC correction is a ten-line notebook.

# COMMAND ----------

repaired = (
    spark.table("dq_quarantine").alias("q")
    .join(spark.table("samples.tpch.orders").alias("s"),
          F.col("q.order_id") == F.col("s.o_orderkey"))
    .select(
        F.col("q.order_id"),
        F.col("s.o_custkey").cast("bigint").alias("customer_id"),          # looked up
        F.when(F.col("q.total_price") >= 600000, F.col("q.total_price") / 1000)
         .otherwise(F.abs(F.col("q.total_price"))).cast("decimal(20,2)").alias("total_price"),
        F.col("q.order_date"),
    )
)
# Gold and quarantine are disjoint by construction, so the re-drive is a plain union.
# Written to a new table rather than appended, so re-running this cell cannot double-count.
repaired = repaired.select(*spark.table("dq_gold_quarantine").columns)
(
    spark.table("dq_gold_quarantine")
    .unionByName(repaired)
    .write.mode("overwrite").saveAsTable("dq_gold_repaired")
)

repaired_total = total_revenue(spark.table("dq_gold_repaired"))
print(f"correct revenue    {truth:>20,.0f}")
print(f"after the re-drive {repaired_total:>20,.0f}")
print(f"remaining error    {100 * (repaired_total - truth) / truth:>19.4f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC Back to the right answer — from the rows the other three pipelines had already thrown
# MAGIC away, failed on, or silently published.
# MAGIC
# MAGIC > The *operational* half of this — who owns the quarantine queue, how often it is
# MAGIC > reviewed, alerting on the failure **rate** rather than the count, and making re-drive
# MAGIC > a routine rather than a heroic Tuesday — is **Lecture 11**. This notebook only argues
# MAGIC > that the table needs to exist.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. What it cost
# MAGIC
# MAGIC The usual objection is that this is extra machinery. It is worth measuring rather than
# MAGIC debating.

# COMMAND ----------

sizes = []
for t in ["dq_gold_quarantine", "dq_quarantine"]:
    d = spark.sql(f"DESCRIBE DETAIL {t}").select("numFiles", "sizeInBytes").first()
    sizes.append((t, d["numFiles"], d["sizeInBytes"]))

gold_bytes = sizes[0][2]
q_bytes = sizes[1][2]
for t, files, b in sizes:
    print(f"{t:<22} {files:>4} files  {b / 1024:>12,.0f} KB")
print(f"\nquarantine overhead: {100 * q_bytes / gold_bytes:.3f}% of the table it protects")

# COMMAND ----------

# MAGIC %md
# MAGIC A fraction of a percent of the storage, one extra write per run, and in exchange you
# MAGIC can answer "is anything missing?" with a number instead of a shrug.
# MAGIC
# MAGIC ### The argument, in one table
# MAGIC
# MAGIC | | ignore | fail | drop | quarantine |
# MAGIC |---|---|---|---|---|
# MAGIC | publishes a correct number | no | — | yes | yes |
# MAGIC | publishes *anything* | yes | no | yes | yes |
# MAGIC | can say what was rejected | no | no | no | **yes** |
# MAGIC | can recover the rejected rows | — | — | no | **yes** |
# MAGIC | the books balance | yes | yes | no | **yes** |
# MAGIC
# MAGIC ### Try it
# MAGIC
# MAGIC * Delete the `price_plausible` rule and re-run section 4. Which pipeline changes most?
# MAGIC * Change the 1000× defect to 10×. At what multiplier does the error stop being
# MAGIC   obvious — and would a rule still catch it?
# MAGIC * The duplicates never reach quarantine; `dropDuplicates` removes them silently. Is
# MAGIC   that the right call, and how would you make the count visible?
# MAGIC * Only one of these four pipelines lets you answer "was last Tuesday's report wrong
# MAGIC   too?". Which, and what would you have to store to answer it properly?
