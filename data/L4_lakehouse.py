# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 4 — A medallion lakehouse on TPC-H
# MAGIC
# MAGIC A walkthrough, not an assignment. Nothing here is graded and there is nothing to
# MAGIC submit. Run it top to bottom, then come back and break things.
# MAGIC
# MAGIC Every section does two jobs: it shows you **how** a layer is built, and then shows you
# MAGIC **what that layer bought you** — because the reason to keep three copies of the same
# MAGIC data is not obvious until you see what each one makes possible.
# MAGIC
# MAGIC 1. Explore the source — and find the grain
# MAGIC 2. Bronze — capture
# MAGIC 3. Silver — enforce, and quarantine what fails
# MAGIC 4. Gold — the star schema
# MAGIC 5. SCD Type 2 on `dim_customer`
# MAGIC 6. What the layers cost, and what they bought

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Your own schema, derived from your login. Nothing to edit — just run it.

# COMMAND ----------

import re

from pyspark.sql import functions as F
from pyspark.sql import Window
from delta.tables import DeltaTable

user = spark.sql("SELECT current_user()").first()[0]
schema = "lakehouse_" + re.sub(r"\W+", "_", user.split("@")[0]).lower()

spark.sql(f"CREATE SCHEMA IF NOT EXISTS workspace.{schema}")
spark.sql(f"USE workspace.{schema}")
print(f"Working in workspace.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Explore the source — and find the grain
# MAGIC
# MAGIC `samples.tpch` is the TPC-H decision-support benchmark: a wholesale supplier, eight
# MAGIC tables, present in every Databricks workspace. Nothing to download.

# COMMAND ----------

display(spark.sql("SHOW TABLES IN samples.tpch"))

# COMMAND ----------

for t in ["region", "nation", "customer", "part", "orders", "lineitem"]:
    n = spark.table(f"samples.tpch.{t}").count()
    print(f"{t:<10} {n:>12,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### The grain
# MAGIC
# MAGIC **Grain** is the answer to "one row per *what*?" It is the first question to ask about
# MAGIC any table, and the one that decides your whole model.
# MAGIC
# MAGIC `orders` and `lineitem` look like they describe the same thing. They do not.

# COMMAND ----------

orders = spark.table("samples.tpch.orders")
lineitem = spark.table("samples.tpch.lineitem")

print(f"orders:   {orders.count():>12,} rows, {orders.select('o_orderkey').distinct().count():>12,} distinct order keys")
print(f"lineitem: {lineitem.count():>12,} rows, {lineitem.select('l_orderkey').distinct().count():>12,} distinct order keys")

display(
    lineitem.groupBy("l_orderkey").count()
    .groupBy("count").agg(F.count("*").alias("orders_with_this_many_lines"))
    .orderBy("count")
)

# COMMAND ----------

# MAGIC %md
# MAGIC So: **`orders` is one row per order. `lineitem` is one row per line of an order** —
# MAGIC roughly four lines per order, and one order key repeats across all of them.
# MAGIC
# MAGIC That single fact drives two decisions you will see later:
# MAGIC
# MAGIC * **Silver keeps both grains.** Rolling `lineitem` up to order level in silver would
# MAGIC   permanently destroy your ability to ask "which *parts* sold best" — the part key
# MAGIC   only exists on the line.
# MAGIC * **Gold picks one grain per fact table.** `fct_orders` will be one row per order, and
# MAGIC   the line-level detail gets aggregated *on the way in*. That aggregation is fine here
# MAGIC   precisely because silver still has the detail if anyone needs it.
# MAGIC
# MAGIC > **Try it:** what would the grain of a `fct_lineitem` table be, and which questions
# MAGIC > would it answer that `fct_orders` cannot?

# COMMAND ----------

spark.table("samples.tpch.orders").printSchema()
display(spark.table("samples.tpch.orders").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bronze — capture
# MAGIC
# MAGIC The rule: **everything the source had, plus metadata. Nothing removed, nothing
# MAGIC retyped, no filters.** If the source sends rubbish, bronze keeps the rubbish.
# MAGIC
# MAGIC This is genuinely hard to do, because you will see bad data on the way in and want to
# MAGIC fix it. Don't. Bronze is your evidence: when someone asks in March what the source
# MAGIC actually sent in January, bronze is the only thing that can answer — and if you
# MAGIC cleaned it on the way in, you destroyed the answer.

# COMMAND ----------

# MAGIC %md
# MAGIC ### First, some realistic damage
# MAGIC
# MAGIC TPC-H is pristine, which would make the silver layer a no-op and teach you nothing. So
# MAGIC before we ingest, we break it in four ways that real sources really do break:
# MAGIC
# MAGIC | defect | looks like | what should catch it |
# MAGIC |---|---|---|
# MAGIC | negative value | `-1234.56` where only positives make sense | a range rule |
# MAGIC | missing value | `NULL` in a required field | a not-null rule |
# MAGIC | inconsistent casing | `"  building "` instead of `"BUILDING"` | a domain rule |
# MAGIC | duplicate row | the same row twice | deduplication |
# MAGIC
# MAGIC Defects are placed on **named columns** and a **fixed number of rows**, chosen by key
# MAGIC order — so the damage is identical every run, and even `region` (five rows) gets some.

# COMMAND ----------

# Which column carries which defect, per table. Keeping this explicit means the silver
# rules below are a response to something real, not guesswork.
DEFECTS = {
    "region":   {"key": "r_regionkey", "negative": "r_regionkey", "null": "r_name",     "casing": "r_name"},
    "nation":   {"key": "n_nationkey", "negative": "n_regionkey", "null": "n_name",     "casing": "n_name"},
    "customer": {"key": "c_custkey",   "negative": "c_nationkey", "null": "c_name",     "casing": "c_mktsegment"},
    "part":     {"key": "p_partkey",   "negative": "p_retailprice", "null": "p_name",   "casing": "p_container"},
    "orders":   {"key": "o_orderkey",  "negative": "o_totalprice", "null": "o_orderstatus", "casing": "o_orderpriority"},
    "lineitem": {"key": "l_orderkey",  "negative": "l_quantity",  "null": "l_returnflag", "casing": "l_shipmode"},
}


def inject_defects(df, spec):
    """Damage a fixed, reproducible set of rows.

    Rows are chosen by taking the lowest key values, so the same rows are hit every run.
    Each defect type gets its own disjoint slice of keys, which keeps the quarantine
    breakdown readable: one failed rule per row.
    """
    key = spec["key"]
    total = df.count()

    # `region` has five rows and `nation` twenty-five. Damaging four disjoint slices of a
    # five-row table leaves nothing behind, so the tiny lookups get the casing defect only.
    kinds = ["negative", "null", "casing", "dup"] if total >= 20 else ["casing", "dup"]

    # ~0.2% of rows per defect type, at least one row, and never more than the table can
    # spare across all the slices we need
    n = max(1, min(int(total * 0.002), 50, total // (len(kinds) + 1)))

    wanted = len(kinds) * n
    key_values = [r[0] for r in df.select(key).distinct().orderBy(key).limit(wanted).collect()]
    slices = {k: key_values[i * n:(i + 1) * n] for i, k in enumerate(kinds)}

    out = df
    if "negative" in slices:
        out = out.withColumn(
            spec["negative"],
            F.when(F.col(key).isin(slices["negative"]), -F.abs(F.col(spec["negative"])))
             .otherwise(F.col(spec["negative"])),
        )
    if "null" in slices:
        out = out.withColumn(
            spec["null"],
            F.when(F.col(key).isin(slices["null"]), F.lit(None)).otherwise(F.col(spec["null"])),
        )
    out = out.withColumn(
        spec["casing"],
        F.when(F.col(key).isin(slices["casing"]),
               F.concat(F.lit("  "), F.lower(F.col(spec["casing"])), F.lit(" ")))
         .otherwise(F.col(spec["casing"])),
    )

    # and a handful of exact duplicate rows, from a slice no other defect touched so the
    # key values are still the ones we selected
    dupes = out.filter(F.col(key).isin(slices["dup"]))
    return out.unionByName(dupes), n, kinds


# COMMAND ----------

for t, spec in DEFECTS.items():
    src = spark.table(f"samples.tpch.{t}")
    dirty, n, kinds = inject_defects(src, spec)
    (
        dirty
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source", F.lit(f"samples.tpch.{t}"))
        .write.mode("overwrite").saveAsTable(f"bronze_{t}")
    )
    defects = ", ".join(k for k in kinds if k != "dup")
    print(f"bronze_{t:<9} {spark.table(f'bronze_{t}').count():>12,} rows  "
          f"({n} row(s) each of: {defects}; plus {n} duplicated)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### What bronze bought you
# MAGIC
# MAGIC Two metadata columns, and suddenly the table can answer questions about *itself*:
# MAGIC what arrived, from where, and when. That is the whole point of the layer.

# COMMAND ----------

display(
    spark.sql("""
        SELECT _source, count(*) AS rows_received, max(_ingested_at) AS last_seen
        FROM bronze_customer GROUP BY _source
        UNION ALL
        SELECT _source, count(*), max(_ingested_at) FROM bronze_orders GROUP BY _source
        UNION ALL
        SELECT _source, count(*), max(_ingested_at) FROM bronze_lineitem GROUP BY _source
        ORDER BY _source
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC The damage is still there, untouched, exactly as "the source sent it":

# COMMAND ----------

display(
    spark.table("bronze_customer")
    .filter("c_name IS NULL OR c_nationkey < 0 OR c_mktsegment <> upper(c_mktsegment)")
    .select("c_custkey", "c_name", "c_nationkey", "c_mktsegment")
    .orderBy("c_custkey")
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver — enforce, and quarantine what fails
# MAGIC
# MAGIC Four jobs, in order: **enforce types**, **deduplicate**, **apply rules**, **quarantine
# MAGIC failures**. And one job it must *not* do: aggregate. Silver preserves the grain.
# MAGIC
# MAGIC The word to notice is **quarantine**, not *drop*. Bad rows go to a separate table with
# MAGIC the reason attached, because a deleted row cannot be investigated, fixed or re-driven.

# COMMAND ----------

def split_valid(df, rules: dict):
    """Split df into (passing, failing) given {rule_name: sql_condition}.

    Note the coalesce: a rule that evaluates to NULL counts as a FAILURE. Section 3.1
    below shows what happens without it.
    """
    conds = [F.coalesce(F.expr(c), F.lit(False)).alias(name) for name, c in rules.items()]
    tagged = df.select("*", *conds)

    all_pass = F.lit(True)
    for name in rules:
        all_pass = all_pass & F.col(name)

    failed_rules = F.concat_ws(",", *[F.when(~F.col(n), F.lit(n)) for n in rules])

    good = tagged.filter(all_pass).drop(*rules.keys())
    bad = (
        tagged.filter(~all_pass)
        .withColumn("_failed_rules", failed_rules)
        .withColumn("_quarantined_at", F.current_timestamp())
        .drop(*rules.keys())
    )
    return good, bad


def build_silver(name, typed, rules):
    """Deduplicate, split, write both halves, report."""
    business_cols = [c for c in typed.columns if not c.startswith("_")]
    before = typed.count()
    deduped = typed.dropDuplicates(business_cols)
    removed = before - deduped.count()

    good, bad = split_valid(deduped, rules)
    good.write.mode("overwrite").saveAsTable(f"silver_{name}")
    bad.write.mode("overwrite").saveAsTable(f"quarantine_{name}")

    print(f"silver_{name:<9} {spark.table(f'silver_{name}').count():>10,} rows   "
          f"quarantined {spark.table(f'quarantine_{name}').count():>6,}   "
          f"duplicates removed {removed:>4,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### customer

# COMMAND ----------

typed_customer = spark.table("bronze_customer").select(
    F.col("c_custkey").cast("bigint").alias("customer_id"),
    F.trim(F.col("c_name")).alias("name"),
    F.trim(F.col("c_address")).alias("address"),
    F.col("c_nationkey").cast("int").alias("nation_id"),
    F.trim(F.col("c_phone")).alias("phone"),
    F.col("c_acctbal").cast("decimal(18,2)").alias("account_balance"),
    F.trim(F.col("c_mktsegment")).alias("market_segment"),
    F.col("_ingested_at"),
    F.col("_source"),
)

customer_rules = {
    "has_id":        "customer_id IS NOT NULL",
    "has_name":      "name IS NOT NULL AND length(name) > 0",
    "valid_nation":  "nation_id >= 0",
    "valid_segment": "market_segment IN ('BUILDING','AUTOMOBILE','MACHINERY','HOUSEHOLD','FURNITURE')",
}

build_silver("customer", typed_customer, customer_rules)
display(spark.table("quarantine_customer").groupBy("_failed_rules").count())

# COMMAND ----------

# MAGIC %md
# MAGIC Each rule caught the defect it was written for, and `_failed_rules` says which. Notice
# MAGIC that `valid_segment` catches the casing damage **only because** silver trims but does
# MAGIC not upper-case: the rule states the domain, and anything outside it is quarantined
# MAGIC rather than silently coerced.
# MAGIC
# MAGIC > **Try it:** change the rule to `upper(market_segment) IN (...)`. The rows stop being
# MAGIC > quarantined — but is that a fix, or did you just hide a broken upstream feed?

# COMMAND ----------

display(spark.table("quarantine_customer").select(
    "customer_id", "name", "nation_id", "market_segment", "_failed_rules").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Why the `coalesce` is there
# MAGIC
# MAGIC `market_segment IN ('BUILDING', ...)` is not `true` or `false` when `market_segment`
# MAGIC is NULL — it is **NULL**. And NULL passes neither `filter(cond)` nor `filter(~cond)`.
# MAGIC
# MAGIC Here is the same split written the naive way.

# COMMAND ----------

naive_cond = F.expr("market_segment IN ('BUILDING','AUTOMOBILE','MACHINERY','HOUSEHOLD','FURNITURE')")
with_nulls = typed_customer.withColumn(
    "market_segment",
    F.when(F.col("customer_id") < 20, F.lit(None)).otherwise(F.col("market_segment")),
)

total = with_nulls.count()
naive_good = with_nulls.filter(naive_cond).count()
naive_bad = with_nulls.filter(~naive_cond).count()

guarded = F.coalesce(naive_cond, F.lit(False))
safe_good = with_nulls.filter(guarded).count()
safe_bad = with_nulls.filter(~guarded).count()

print(f"input rows          {total:,}")
print(f"naive   good + bad = {naive_good:,} + {naive_bad:,} = {naive_good + naive_bad:,}"
      f"   <- {total - naive_good - naive_bad:,} rows vanished")
print(f"guarded good + bad = {safe_good:,} + {safe_bad:,} = {safe_good + safe_bad:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC Rows that vanish are worse than rows that fail. A failing row lands in quarantine where
# MAGIC somebody will see it; a vanished row is absent from both tables and from every count
# MAGIC anyone runs. You get a silent, permanent, unexplained shortfall.
# MAGIC
# MAGIC Lecture 11 comes back to this — it is the single most common data-quality bug there is.

# COMMAND ----------

# MAGIC %md
# MAGIC ### The remaining four tables
# MAGIC
# MAGIC Same shape every time: type it, name the rules, split, write both halves.

# COMMAND ----------

typed_orders = spark.table("bronze_orders").select(
    F.col("o_orderkey").cast("bigint").alias("order_id"),
    F.col("o_custkey").cast("bigint").alias("customer_id"),
    F.trim(F.col("o_orderstatus")).alias("order_status"),
    F.col("o_totalprice").cast("decimal(18,2)").alias("total_price"),
    F.col("o_orderdate").cast("date").alias("order_date"),
    F.trim(F.col("o_orderpriority")).alias("order_priority"),
    F.trim(F.col("o_clerk")).alias("clerk"),
    F.col("_ingested_at"),
    F.col("_source"),
)

orders_rules = {
    "has_id":         "order_id IS NOT NULL",
    "has_customer":   "customer_id IS NOT NULL AND customer_id > 0",
    "positive_price": "total_price > 0",
    "has_status":     "order_status IS NOT NULL",
    "valid_priority": "order_priority IN ('1-URGENT','2-HIGH','3-MEDIUM','4-NOT SPECIFIED','5-LOW')",
}

build_silver("orders", typed_orders, orders_rules)

# COMMAND ----------

typed_lineitem = spark.table("bronze_lineitem").select(
    F.col("l_orderkey").cast("bigint").alias("order_id"),
    F.col("l_partkey").cast("bigint").alias("part_id"),
    F.col("l_linenumber").cast("int").alias("line_number"),
    F.col("l_quantity").cast("decimal(18,2)").alias("quantity"),
    F.col("l_extendedprice").cast("decimal(18,2)").alias("extended_price"),
    F.col("l_discount").cast("decimal(18,4)").alias("discount"),
    F.trim(F.col("l_returnflag")).alias("return_flag"),
    F.col("l_shipdate").cast("date").alias("ship_date"),
    F.trim(F.col("l_shipmode")).alias("ship_mode"),
    F.col("_ingested_at"),
    F.col("_source"),
)

lineitem_rules = {
    "has_order":         "order_id IS NOT NULL",
    "positive_quantity": "quantity > 0",
    "discount_in_range": "discount BETWEEN 0 AND 1",
    "has_return_flag":   "return_flag IS NOT NULL",
    "valid_shipmode":    "ship_mode IN ('AIR','AIR REG','RAIL','SHIP','TRUCK','MAIL','FOB','REG AIR')",
}

build_silver("lineitem", typed_lineitem, lineitem_rules)

# COMMAND ----------

typed_part = spark.table("bronze_part").select(
    F.col("p_partkey").cast("bigint").alias("part_id"),
    F.trim(F.col("p_name")).alias("name"),
    F.trim(F.col("p_mfgr")).alias("manufacturer"),
    F.trim(F.col("p_brand")).alias("brand"),
    F.trim(F.col("p_type")).alias("type"),
    F.col("p_size").cast("int").alias("size"),
    F.trim(F.col("p_container")).alias("container"),
    F.col("p_retailprice").cast("decimal(18,2)").alias("retail_price"),
    F.col("_ingested_at"),
    F.col("_source"),
)

part_rules = {
    "has_id":              "part_id IS NOT NULL",
    "has_name":            "name IS NOT NULL AND length(name) > 0",
    "positive_price":      "retail_price > 0",
    "canonical_container": "container = upper(container)",
}

build_silver("part", typed_part, part_rules)

# COMMAND ----------

typed_nation = spark.table("bronze_nation").select(
    F.col("n_nationkey").cast("int").alias("nation_id"),
    F.trim(F.col("n_name")).alias("name"),
    F.col("n_regionkey").cast("int").alias("region_id"),
    F.col("_ingested_at"),
    F.col("_source"),
)

nation_rules = {
    "has_id":         "nation_id >= 0",
    "has_name":       "name IS NOT NULL AND length(name) > 0",
    "canonical_name": "name = upper(name)",
    "valid_region":   "region_id >= 0",
}

build_silver("nation", typed_nation, nation_rules)

# COMMAND ----------

typed_region = spark.table("bronze_region").select(
    F.col("r_regionkey").cast("int").alias("region_id"),
    F.trim(F.col("r_name")).alias("name"),
    F.col("_ingested_at"),
    F.col("_source"),
)

region_rules = {
    "has_id":         "region_id >= 0",
    "has_name":       "name IS NOT NULL AND length(name) > 0",
    "canonical_name": "name = upper(name)",
}

build_silver("region", typed_region, region_rules)

# COMMAND ----------

# MAGIC %md
# MAGIC Two of `region`'s three rules never fire on this data, and that is fine. A rule
# MAGIC documents what the business believes must be true; it is not scored on how many rows
# MAGIC it catches today. The one you never see fire is the one protecting you on the day the
# MAGIC source changes.

# COMMAND ----------

# MAGIC %md
# MAGIC ### What silver bought you
# MAGIC
# MAGIC Trustworthy tables at the original grain — and, just as valuable, a **written record of
# MAGIC what was wrong**. The quarantine tables are the input to a data-quality conversation
# MAGIC with whoever owns the source.
# MAGIC
# MAGIC `region` has five rows, so one bad row is 20% of the table. The percentage is only
# MAGIC meaningful for the large tables; the *count* is what matters for the small ones.

# COMMAND ----------

rows = []
for t in DEFECTS:
    s = spark.table(f"silver_{t}").count()
    q = spark.table(f"quarantine_{t}").count()
    rows.append((t, s, q, round(100 * q / (s + q), 3)))

display(spark.createDataFrame(
    rows, "table string, silver_rows long, quarantined long, pct_quarantined double"))

# COMMAND ----------

display(
    spark.table("quarantine_orders")
    .groupBy("_failed_rules").count().orderBy(F.desc("count"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold — the star schema
# MAGIC
# MAGIC Silver is correct but shaped like the source: normalised, transactional, built for
# MAGIC writing. Gold is shaped for the question: one fact table of numbers, surrounded by
# MAGIC dimension tables of meaning, one join deep.

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_date
# MAGIC
# MAGIC Every warehouse has one. It is pure generated data — no source system involved — and it
# MAGIC exists so that "group by quarter" or "exclude weekends" is a join rather than a pile of
# MAGIC date functions repeated in forty different queries.
# MAGIC
# MAGIC It is also the classic **role-playing dimension**: the same table joins as order date,
# MAGIC ship date and receipt date.

# COMMAND ----------

dates = spark.sql("""
SELECT
  CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key,
  d                                       AS date,
  year(d)                                 AS year,
  quarter(d)                              AS quarter,
  month(d)                                AS month,
  date_format(d, 'MMMM')                  AS month_name,
  dayofmonth(d)                           AS day,
  dayofweek(d)                            AS day_of_week,
  date_format(d, 'EEEE')                  AS day_name,
  CASE WHEN dayofweek(d) IN (1, 7) THEN true ELSE false END AS is_weekend
FROM (
  SELECT explode(sequence(DATE'1992-01-01', DATE'1999-12-31', INTERVAL 1 DAY)) AS d
)
""")
dates.write.mode("overwrite").saveAsTable("dim_date")
print(f"dim_date: {spark.table('dim_date').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_part
# MAGIC
# MAGIC A plain Type 1 dimension: descriptive attributes, a business key, no history. Note the
# MAGIC deliberate **denormalisation** — brand, type and container sit here rather than in three
# MAGIC further tables. That is the star/snowflake choice, made in favour of the star.

# COMMAND ----------

(
    spark.table("silver_part")
    .select("part_id", "name", "manufacturer", "brand", "type", "size",
            "container", "retail_price")
    .write.mode("overwrite").saveAsTable("dim_part")
)
print(f"dim_part: {spark.table('dim_part').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### fct_orders
# MAGIC
# MAGIC **Grain: one row per order.** Say it out loud before writing a fact table, because every
# MAGIC measure on it has to be additive at that grain.
# MAGIC
# MAGIC This is where we aggregate `lineitem` up to order level — legitimate *here*, and not in
# MAGIC silver, because silver still holds the line-level detail for anyone who needs it.
# MAGIC
# MAGIC `order_id` is the key. Lab 8 declares a primary key on that exact name.

# COMMAND ----------

line_agg = (
    spark.table("silver_lineitem")
    .groupBy("order_id")
    .agg(
        F.count("*").alias("line_item_count"),
        F.sum("quantity").cast("decimal(18,2)").alias("total_quantity"),
        F.sum("extended_price").cast("decimal(18,2)").alias("gross_amount"),
        F.sum(F.col("extended_price") * F.col("discount")).cast("decimal(18,2)")
            .alias("total_discount_amount"),
        F.sum(F.col("extended_price") * (F.lit(1) - F.col("discount"))).cast("decimal(18,2)")
            .alias("net_revenue"),
        F.avg("discount").cast("decimal(18,4)").alias("avg_discount"),
    )
)

fct_orders = (
    spark.table("silver_orders").alias("o")
    .join(line_agg.alias("l"), "order_id", "inner")
    .select(
        F.col("o.order_id"),
        F.col("o.customer_id"),
        F.date_format(F.col("o.order_date"), "yyyyMMdd").cast("int").alias("order_date_key"),
        F.col("o.order_status"),
        F.col("o.total_price"),
        F.col("l.line_item_count"),
        F.col("l.total_quantity"),
        F.col("l.gross_amount"),
        F.col("l.total_discount_amount"),
        F.col("l.net_revenue"),
        F.col("l.avg_discount"),
    )
)
fct_orders.write.mode("overwrite").saveAsTable("fct_orders")
print(f"fct_orders: {spark.table('fct_orders').count():,} rows")
display(spark.table("fct_orders").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Additive and non-additive measures
# MAGIC
# MAGIC Look at the last two columns. They came from the same source and they behave completely
# MAGIC differently:
# MAGIC
# MAGIC * `total_discount_amount` is **additive** — sum it across any dimension and the answer
# MAGIC   is correct.
# MAGIC * `avg_discount` is **not** — it is a ratio. Summing it is meaningless, and averaging
# MAGIC   the averages is wrong whenever orders have different numbers of lines.
# MAGIC
# MAGIC A dashboard will happily do both. Here is the size of the error.

# COMMAND ----------

display(spark.sql("""
SELECT
  d.year,
  avg(f.avg_discount)                                AS average_of_averages,
  sum(f.total_discount_amount) / sum(f.gross_amount) AS true_weighted_discount
FROM fct_orders f
JOIN dim_date d ON f.order_date_key = d.date_key
GROUP BY d.year ORDER BY d.year
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC > **Try it:** the safe habit is to store the **numerator and denominator** as additive
# MAGIC > measures and let the ratio be computed at query time. What two columns would you add
# MAGIC > to `fct_orders` so `avg_discount` never has to be stored at all?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. SCD Type 2 on dim_customer
# MAGIC
# MAGIC Customers move house. Customers unsubscribe. The question is what your dimension does
# MAGIC about it — and the wrong answer quietly corrupts every historical report you have.

# COMMAND ----------

# MAGIC %md
# MAGIC ### The initial load
# MAGIC
# MAGIC One current row per customer. Five columns carry the history:
# MAGIC
# MAGIC * **`sk`** — the surrogate key. Unique per *row*, not per customer, because
# MAGIC   `customer_id` will stop being unique the moment anything changes. Anything that needs
# MAGIC   a specific *version* joins on this.
# MAGIC * **`is_current`** — a flag marking the row that is true today. Pure denormalisation for
# MAGIC   query convenience, and worth it: "customers as they are now" becomes a filter.
# MAGIC * **`is_active`** — a business attribute, not a technical one. Tracked like any other.
# MAGIC * **`effective_date` / `expiry_date`** — the interval this version was true for.
# MAGIC
# MAGIC **The convention, and the most common place to get SCD 2 wrong:** the interval is
# MAGIC `[effective_date, expiry_date)`. Effective date **inclusive**, expiry **exclusive**,
# MAGIC NULL expiry for the current row. A closed row's `expiry_date` equals the next row's
# MAGIC `effective_date` — the same date appears twice and belongs to the *later* version.

# COMMAND ----------

initial = (
    spark.table("silver_customer")
    .select("customer_id", "name", "address", "nation_id", "market_segment")
    # row_number, not monotonically_increasing_id: contiguous, reproducible, and easy to
    # continue from when we add rows below.
    .withColumn("sk", F.row_number().over(Window.orderBy("customer_id")).cast("bigint"))
    .withColumn("is_active", F.lit(True))
    .withColumn("is_current", F.lit(True))
    .withColumn("effective_date", F.lit("1992-01-01").cast("date"))
    .withColumn("expiry_date", F.lit(None).cast("date"))
)
initial.write.mode("overwrite").saveAsTable("dim_customer")
print(f"dim_customer: {spark.table('dim_customer').count():,} rows")
display(spark.table("dim_customer").orderBy("sk").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### A batch of changes arrives
# MAGIC
# MAGIC Two kinds, matching the two scenarios from the lecture: customers who **moved**, and
# MAGIC customers who **unsubscribed**.

# COMMAND ----------

CHANGE_DATE = "1997-06-15"

current = spark.table("dim_customer").filter("is_current")

movers = (
    current.filter("customer_id % 50 = 0")
    .select("customer_id", "name", "nation_id", "market_segment")
    .withColumn("address", F.concat(F.lit("NEW ADDRESS "), F.col("customer_id").cast("string")))
    .withColumn("is_active", F.lit(True))
)

leavers = (
    current.filter("customer_id % 50 = 7")
    .select("customer_id", "name", "address", "nation_id", "market_segment")
    .withColumn("is_active", F.lit(False))
)

changes = (
    movers.select("customer_id", "name", "address", "nation_id", "market_segment", "is_active")
    .unionByName(
        leavers.select("customer_id", "name", "address", "nation_id", "market_segment", "is_active"))
    .withColumn("change_date", F.lit(CHANGE_DATE).cast("date"))
)
changes.write.mode("overwrite").saveAsTable("customer_changes")
changes = spark.table("customer_changes")   # materialised, so it cannot drift

print(f"{movers.count():,} moved, {leavers.count():,} unsubscribed, on {CHANGE_DATE}")
display(changes.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why this needs two passes
# MAGIC
# MAGIC For each changed customer, **two** things must happen:
# MAGIC
# MAGIC 1. the existing current row is **closed** — `is_current = false`, `expiry_date = change_date`
# MAGIC 2. a new row is **inserted** — new `sk`, new values, `is_current = true`,
# MAGIC    `effective_date = change_date`, `expiry_date = NULL`
# MAGIC
# MAGIC A single `MERGE` cannot do both for the same match key: a matched row is either updated
# MAGIC or deleted, never updated *and* used to insert a sibling. So: **close first, insert
# MAGIC second.**
# MAGIC
# MAGIC (The other common trick is one MERGE over a union of the change set with itself, where
# MAGIC half the rows carry a NULL merge key so they fall through to `WHEN NOT MATCHED`. Same
# MAGIC result, harder to read.)

# COMMAND ----------

# Capture the high-water mark BEFORE touching the table, so new keys cannot collide.
max_sk = spark.table("dim_customer").agg(F.max("sk")).first()[0]
print(f"highest surrogate key before the merge: {max_sk:,}")

# COMMAND ----------

# --- pass 1: close the rows that changed ---------------------------------------------

dim = DeltaTable.forName(spark, f"workspace.{schema}.dim_customer")

(
    dim.alias("t")
    .merge(changes.alias("s"), "t.customer_id = s.customer_id AND t.is_current = true")
    .whenMatchedUpdate(set={
        "is_current":  F.lit(False),
        "expiry_date": F.col("s.change_date"),
    })
    .execute()
)

closed = spark.table("dim_customer").filter("is_current = false").count()
print(f"rows closed: {closed:,}")

# COMMAND ----------

# --- pass 2: open the new versions ----------------------------------------------------

new_versions = (
    changes
    .withColumn("sk", (F.lit(max_sk) + F.row_number().over(Window.orderBy("customer_id"))).cast("bigint"))
    .withColumn("is_current", F.lit(True))
    .withColumnRenamed("change_date", "effective_date")
    .withColumn("expiry_date", F.lit(None).cast("date"))
    .select("customer_id", "name", "address", "nation_id", "market_segment",
            "sk", "is_active", "is_current", "effective_date", "expiry_date")
)

new_versions.write.mode("append").saveAsTable("dim_customer")
print(f"dim_customer: {spark.table('dim_customer').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### The result
# MAGIC
# MAGIC One customer who moved, and one who unsubscribed. Both have two rows; in both cases the
# MAGIC old row is closed and the new one is open.

# COMMAND ----------

moved_id = movers.select("customer_id").orderBy("customer_id").first()[0]
left_id = leavers.select("customer_id").orderBy("customer_id").first()[0]

display(
    spark.table("dim_customer")
    .filter(F.col("customer_id").isin([moved_id, left_id]))
    .select("sk", "customer_id", "name", "address", "is_active", "is_current",
            "effective_date", "expiry_date")
    .orderBy("customer_id", "effective_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Look at the second customer: **the address did not change.** The *status* did, and
# MAGIC it produced a new row exactly like the move did.
# MAGIC
# MAGIC That is worth naming: **a status change is a change like any other.** You do not need a
# MAGIC separate mechanism for deletions or deactivations. The customer who left gets a version
# MAGIC that says so, and the earlier rows remain valid for the period they described — which is
# MAGIC why "how many active customers did we have last March" is answerable at all.

# COMMAND ----------

# A few integrity properties worth checking on any SCD 2 table you build.
dc = spark.table("dim_customer")
print("surrogate keys unique:      ",
      dc.select("sk").distinct().count() == dc.count())
print("one current row per customer:",
      dc.filter("is_current").count() == dc.select("customer_id").distinct().count())
print("no current row has expired: ",
      dc.filter("is_current AND expiry_date IS NOT NULL").count() == 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ### What SCD Type 2 bought you: the point-in-time query
# MAGIC
# MAGIC This is the payoff for the whole section. Note the boundary handling — `>` on the
# MAGIC expiry, not `>=`, because the interval is half-open.

# COMMAND ----------

def address_as_of(customer_id, as_of):
    return spark.sql(f"""
        SELECT '{as_of}' AS as_of, customer_id, address, is_active
        FROM dim_customer
        WHERE customer_id = {customer_id}
          AND effective_date <= DATE'{as_of}'
          AND (expiry_date IS NULL OR expiry_date > DATE'{as_of}')
    """)

display(
    address_as_of(moved_id, "1995-03-01")
    .unionByName(address_as_of(moved_id, "1998-03-01"))
    .unionByName(address_as_of(left_id, "1995-03-01"))
    .unionByName(address_as_of(left_id, "1998-03-01"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### The same thing against a Type 1 dimension
# MAGIC
# MAGIC Type 1 overwrites. There is only ever one row per customer, so there is no as-of clause
# MAGIC to write — the query *cannot be expressed*.

# COMMAND ----------

(
    spark.table("dim_customer").filter("is_current")
    .select("customer_id", "name", "address", "nation_id", "market_segment", "is_active")
    .write.mode("overwrite").saveAsTable("dim_customer_scd1")
)

display(spark.sql(f"""
    SELECT customer_id, address, is_active FROM dim_customer_scd1
    WHERE customer_id IN ({moved_id}, {left_id})
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Both customers now look as though they have *always* lived at the new address and, for
# MAGIC one of them, as though they were never a customer at all.
# MAGIC
# MAGIC Every historical order they placed will be reported against today's address. Your
# MAGIC regional sales figures for 1995 change retrospectively, every time somebody moves —
# MAGIC quietly, with nothing in any log, and nobody notices the numbers are wrong.
# MAGIC
# MAGIC > **Try it:** a fact table should join to `dim_customer.sk`, not to `customer_id`, so
# MAGIC > each order resolves to the customer *as they were at the time*. `fct_orders` above
# MAGIC > joins on `customer_id` — what breaks, and what would you change?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. What the layers cost, and what they bought
# MAGIC
# MAGIC Three copies of the same data is a real cost. Here is what it buys: the same business
# MAGIC question, asked against the raw source and against gold.

# COMMAND ----------

import time

raw_sql = """
SELECT c.c_mktsegment AS market_segment, year(o.o_orderdate) AS year,
       sum(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM samples.tpch.orders o
JOIN samples.tpch.lineitem l ON o.o_orderkey = l.l_orderkey
JOIN samples.tpch.customer c ON o.o_custkey  = c.c_custkey
GROUP BY c.c_mktsegment, year(o.o_orderdate)
ORDER BY market_segment, year
"""

gold_sql = """
SELECT c.market_segment, d.year, sum(f.net_revenue) AS revenue
FROM fct_orders f
JOIN dim_customer c ON f.customer_id = c.customer_id AND c.is_current
JOIN dim_date     d ON f.order_date_key = d.date_key
GROUP BY c.market_segment, d.year
ORDER BY market_segment, year
"""

for label, sql in [("raw source", raw_sql), ("gold", gold_sql)]:
    t0 = time.time()
    n = spark.sql(sql).count()
    print(f"{label:<12} {len(sql.split()):>4} words, {time.time() - t0:>6.1f}s, {n} rows")

# COMMAND ----------

display(spark.sql(gold_sql))

# COMMAND ----------

# MAGIC %md
# MAGIC The two revenue figures are close but not identical, and that is not a bug: gold is
# MAGIC built from silver, and silver quarantined the damaged rows. The raw query happily
# MAGIC includes the negative quantities and the orphaned keys. Which number would you rather
# MAGIC put in front of a stakeholder?
# MAGIC
# MAGIC The gold query is also shorter, faster, and — more important than either — **readable
# MAGIC by someone who does not know the source system**. `market_segment` and `year` are business
# MAGIC words; `c_mktsegment` and `year(o_orderdate)` are schema trivia.
# MAGIC
# MAGIC That readability is the whole subject of Lecture 8: a gold table with proper names and
# MAGIC column comments is the difference between self-service analytics that works and one that
# MAGIC produces confidently wrong answers.
# MAGIC
# MAGIC ### Where to go from here
# MAGIC
# MAGIC Things worth trying on top of what you just built:
# MAGIC
# MAGIC * Build `fct_lineitem` at line grain and join it to `dim_part`. Which questions does it
# MAGIC   answer that `fct_orders` cannot?
# MAGIC * Make `fct_orders` join `dim_customer` on `sk` instead of `customer_id`, so history
# MAGIC   resolves correctly.
# MAGIC * Add a second batch of changes on a later date and re-run the two-pass merge. Do you
# MAGIC   get three versions for a customer who moved twice?
# MAGIC * Nearly every downstream query filters on `is_current`. Delta has no secondary
# MAGIC   indexes — look at liquid clustering, `ZORDER` and partitioning, and work out which (if
# MAGIC   any) is worth it for a two-valued column.
# MAGIC * Snowflake `dim_customer` by splitting nation and region out. Measure whether the
# MAGIC   storage saving is worth the extra join.
