# Databricks notebook source
# MAGIC %md
# MAGIC # Bulk Historical Price Pipeline (Spark)
# MAGIC
# MAGIC This notebook is the capstone's required Spark data pipeline. It:
# MAGIC 1. Reads the distinct set of watched tickers from `watchlist_tickers` in Lakebase.
# MAGIC 2. Uses Spark to fetch historical daily price bars for each ticker from the
# MAGIC    Massive Stocks API IN PARALLEL, distributing one API call per ticker
# MAGIC    across the cluster's executors (via `rdd.mapPartitions`).
# MAGIC 3. Computes a real transformation with Spark - daily return % and a
# MAGIC    7-day moving average - using window functions partitioned by symbol.
# MAGIC 4. Writes the final rows into `price_snapshots` in Lakebase via
# MAGIC    psycopg2 + execute_values, NOT spark.write.jdbc (which does not
# MAGIC    work reliably against Lakebase in this environment - same
# MAGIC    constraint learned in the Day 2 homework).
# MAGIC
# MAGIC Run this as a scheduled Databricks Job to keep price_snapshots fresh
# MAGIC for every ticker any user is currently watching.

# COMMAND ----------

# DBTITLE 1,Config
dbutils.widgets.text("lookback_days", "90", "Days of history to fetch per ticker")
dbutils.widgets.text("massive_secret_scope", "database", "Massive API secret scope")
dbutils.widgets.text("massive_secret_key", "massive-api-key", "Massive API secret key")

LOOKBACK_DAYS = int(dbutils.widgets.get("lookback_days"))
MASSIVE_SECRET_SCOPE = dbutils.widgets.get("massive_secret_scope")
MASSIVE_SECRET_KEY = dbutils.widgets.get("massive_secret_key")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read the set of tickers to refresh
# MAGIC
# MAGIC Same pattern as the Day 3 embeddings notebook reading `watchlist` -
# MAGIC the pipeline only bothers fetching history for tickers someone is
# MAGIC actually tracking, not the entire market.

# COMMAND ----------

import base64
import psycopg2
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()


def _lakebase_url() -> str:
    secret = w.secrets.get_secret(scope="database", key="lakebase-url")
    return base64.b64decode(secret.value).decode("utf-8")


def get_watched_symbols() -> list[str]:
    conn = psycopg2.connect(_lakebase_url())
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT symbol FROM watchlist_tickers")
        return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


symbols = get_watched_symbols()
print(f"Found {len(symbols)} distinct watched tickers: {symbols}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch historical bars in parallel with Spark
# MAGIC
# MAGIC Uses `mapInPandas` (a DataFrame-level API) instead of
# MAGIC `sparkContext.parallelize(...).mapPartitions(...)`, since the raw
# MAGIC RDD/SparkContext API is not available on Databricks Serverless
# MAGIC compute. `mapInPandas` still distributes the work across
# MAGIC partitions - each partition independently calls the Massive API for
# MAGIC its share of tickers - without needing direct SparkContext access.

# COMMAND ----------

from datetime import date, timedelta

from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
import pandas as pd

from_date = (date.today() - timedelta(days=LOOKBACK_DAYS)).isoformat()
to_date = date.today().isoformat()

symbols_df = spark.createDataFrame([(s,) for s in symbols], ["symbol"])

output_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("epoch_ms", LongType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True),
])

# Fetch the API key ONCE on the driver, where WorkspaceClient() already
# works (notebook auto-auth context). mapInPandas worker processes run
# on executors WITHOUT that auto-auth context, so calling
# WorkspaceClient() inside fetch_bars_pandas fails - instead, capture the
# already-decoded key as a plain string in the closure below.
import base64 as _b64
_massive_secret = w.secrets.get_secret(scope=MASSIVE_SECRET_SCOPE, key=MASSIVE_SECRET_KEY)
_massive_api_key = _b64.b64decode(_massive_secret.value).decode("utf-8")


def fetch_bars_pandas(iterator):
    """
    Runs on each partition: fetch historical bars for this partition's
    symbols from the Massive Stocks API. Uses the API key captured in
    the closure above (_massive_api_key), NOT a fresh WorkspaceClient()
    call, since worker processes lack the driver's auto-auth context.
    """
    import requests

    api_key = _massive_api_key

    for pdf in iterator:
        rows = []
        for symbol in pdf["symbol"]:
            try:
                resp = requests.get(
                    f"https://api.massive.com/v2/aggs/ticker/{symbol}/range/1/day/{from_date}/{to_date}",
                    params={"apiKey": api_key},
                    timeout=15,
                )
                resp.raise_for_status()
                data = resp.json()
                for bar in data.get("results") or []:
                    rows.append({
                        "symbol": symbol,
                        "epoch_ms": bar.get("t"),
                        "open": bar.get("o"),
                        "high": bar.get("h"),
                        "low": bar.get("l"),
                        "close": bar.get("c"),
                        "volume": bar.get("v"),
                    })
            except Exception as exc:
                print(f"Skipping {symbol}: {exc}")
                continue

        yield pd.DataFrame(rows, columns=["symbol", "epoch_ms", "open", "high", "low", "close", "volume"])


bars_df = symbols_df.mapInPandas(fetch_bars_pandas, schema=output_schema)
print(f"Fetched {bars_df.count()} daily bars across {len(symbols)} tickers.")
display(bars_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: daily return % and 7-day moving average
# MAGIC
# MAGIC This is the pipeline's real Spark transformation step - window
# MAGIC functions partitioned by symbol, ordered by date, computing values
# MAGIC that depend on each ticker's own price history.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

bars_df = bars_df.withColumn(
    "snapshot_time", (F.col("epoch_ms") / 1000).cast("timestamp")
)

symbol_window = Window.partitionBy("symbol").orderBy("snapshot_time")

transformed_df = (
    bars_df
    .withColumn("prev_close", F.lag("close").over(symbol_window))
    .withColumn(
        "daily_return_pct",
        F.round(((F.col("close") - F.col("prev_close")) / F.col("prev_close")) * 100, 4),
    )
    .withColumn(
        "moving_avg_7d",
        F.round(F.avg("close").over(symbol_window.rowsBetween(-6, 0)), 4),
    )
)

display(transformed_df.orderBy("symbol", "snapshot_time").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Lakebase via psycopg2 (not spark.write.jdbc)
# MAGIC
# MAGIC Collect the transformed rows back to the driver and batch-insert with
# MAGIC execute_values, same pattern as every other Lakebase write in this
# MAGIC course.

# COMMAND ----------

from psycopg2.extras import execute_values

rows = transformed_df.select(
    "symbol", "snapshot_time", "close", "volume", "daily_return_pct", "moving_avg_7d"
).collect()

print(f"Writing {len(rows)} rows to price_snapshots...")

conn = psycopg2.connect(_lakebase_url())
try:
    cur = conn.cursor()
    insert_sql = """
        INSERT INTO price_snapshots (
            symbol, snapshot_time, price, volume, daily_return_pct, moving_avg_7d, source
        )
        VALUES %s
        ON CONFLICT (symbol, snapshot_time) DO UPDATE
            SET price = EXCLUDED.price,
                volume = EXCLUDED.volume,
                daily_return_pct = EXCLUDED.daily_return_pct,
                moving_avg_7d = EXCLUDED.moving_avg_7d
    """
    data = [
        (
            r["symbol"], r["snapshot_time"], r["close"], r["volume"],
            r["daily_return_pct"], r["moving_avg_7d"], "massive",
        )
        for r in rows
    ]
    execute_values(cur, insert_sql, data, page_size=200)
    conn.commit()
    print(f"Wrote {len(data)} rows into price_snapshots.")
finally:
    conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC `price_snapshots` now carries `daily_return_pct` and `moving_avg_7d`
# MAGIC alongside `price`/`volume`, so the agent's "summarize recent
# MAGIC performance" tool can read these directly instead of recomputing them.
