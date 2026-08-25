# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver Layer: Clean & Enrich → S3 (Serverless Compatible)

# COMMAND ----------

import json
import os
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# Load .env via DataFrame API
BUCKET = "s3://aws-s3-bucket-916491575117-eu-north-1-an"

for row in spark.read.text(f"{BUCKET}/config/.env").collect():
    line = row.value.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        os.environ[k.strip()] = v.strip()

# Hardcoded paths
PATHS = {
    "bronze_binance": f"{BUCKET}/bronze/binance_trades",
    "bronze_fred":    f"{BUCKET}/bronze/fred_macro",
    "silver_trades":  f"{BUCKET}/silver/trades_cleaned",
    "silver_macro":   f"{BUCKET}/silver/macro_indicators",
    "silver_enriched":f"{BUCKET}/silver/trades_enriched",
    "gold_vwap":      f"{BUCKET}/gold/vwap_5min",
    "gold_sr":        f"{BUCKET}/gold/support_resistance",
    "gold_llm":       f"{BUCKET}/gold/llm_training_pairs"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Clean Trades (Dedupe + Validate)

# COMMAND ----------

bronze_df = spark.read.format("delta").load(PATHS["bronze_binance"])
print(f"Bronze count: {bronze_df.count()}")

silver_trades = (bronze_df
    .dropDuplicates(["id", "symbol"])
    .filter(F.col("price").isNotNull())
    .filter(F.col("qty").isNotNull())
    .filter(F.col("price") > 0)
    .withColumn("trade_timestamp", F.from_unixtime(F.col("time")/1000).cast("timestamp"))
    .withColumn("trade_id", F.col("id").cast("long"))
    .withColumn("is_buyer_maker", F.col("isBuyerMaker").cast("boolean"))
    .withColumn("quality_flag", 
        F.when(F.col("isBestMatch") == True, F.lit("valid")).otherwise(F.lit("suspicious")))
    .select("trade_id", "symbol", "price", "qty", "quoteQty",
            "trade_timestamp", "is_buyer_maker", "quality_flag",
            "ingestion_time", "source", "trade_date")
)

(silver_trades.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(PATHS["silver_trades"]))

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver_trades_cleaned
    USING DELTA
    LOCATION '{PATHS["silver_trades"]}'
""")

print(f"✅ Silver trades: {silver_trades.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Clean Macro

# COMMAND ----------

try:
    bronze_macro = spark.read.format("delta").load(PATHS["bronze_fred"])
    silver_macro = (bronze_macro
        .filter(F.col("value").isNotNull())
        .withColumn("date", F.to_date(F.col("date")))
        .select("date", "series_id", "indicator_name", "value", "ingestion_time"))

    (silver_macro.write
        .format("delta")
        .mode("overwrite")
        .save(PATHS["silver_macro"]))

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS silver_macro_indicators
        USING DELTA
        LOCATION '{PATHS["silver_macro"]}'
    """)

    print(f"✅ Silver macro: {silver_macro.count()}")
except Exception as e:
    print(f"⚠️ Macro skipped: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Technical Indicators (Window Functions)

# COMMAND ----------

silver_df = spark.read.format("delta").load(PATHS["silver_trades"])

w50 = Window.partitionBy("symbol").orderBy("trade_timestamp").rowsBetween(-49, 0)
w200 = Window.partitionBy("symbol").orderBy("trade_timestamp").rowsBetween(-199, 0)

silver_enriched = (silver_df
    .withColumn("price_double", F.col("price").cast("double"))
    .withColumn("qty_double", F.col("qty").cast("double"))
    .withColumn("sma_50", F.avg("price_double").over(w50))
    .withColumn("sma_200", F.avg("price_double").over(w200))
    .withColumn("volatility_50", F.stddev("price_double").over(w50))
    .withColumn("volume_50", F.sum("qty_double").over(w50))
    .withColumn("avg_qty_50", F.avg("qty_double").over(w50))
    .withColumn("price_momentum", 
        F.col("price_double") - F.lag("price_double", 50).over(
            Window.partitionBy("symbol").orderBy("trade_timestamp")))
    .withColumn("vwap_50", 
        F.sum(F.col("price_double") * F.col("qty_double")).over(w50) / 
        F.sum("qty_double").over(w50))
)

(silver_enriched.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(PATHS["silver_enriched"]))

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver_trades_enriched
    USING DELTA
    LOCATION '{PATHS["silver_enriched"]}'
""")

print("✅ Silver enriched complete")
display(silver_enriched.limit(5))
