# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold Layer: Aggregates & AI-Ready → S3 (Serverless Compatible)

# COMMAND ----------

import json
import os
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# Load .env + config via DataFrame API
BUCKET = "s3://aws-s3-bucket-916491575117-eu-north-1-an"

for row in spark.read.text(f"{BUCKET}/config/.env").collect():
    line = row.value.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        os.environ[k.strip()] = v.strip()

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
# MAGIC ## 3.1 Gold VWAP 5-Min

# COMMAND ----------

silver_df = spark.read.format("delta").load(PATHS["silver_enriched"])

gold_vwap = (silver_df
    .withColumn("price_d", F.col("price").cast("double"))
    .withColumn("qty_d", F.col("qty").cast("double"))
    .withColumn("window_5min", F.window("trade_timestamp", "5 minutes"))
    .groupBy("symbol", "window_5min")
    .agg(
        (F.sum(F.col("price_d") * F.col("qty_d")) / F.sum("qty_d")).alias("vwap"),
        F.avg("price_d").alias("avg_price"),
        F.min("price_d").alias("low_price"),
        F.max("price_d").alias("high_price"),
        F.stddev("price_d").alias("price_volatility"),
        F.sum("qty_d").alias("total_volume"),
        F.count("*").alias("trade_count"),
        F.sum(F.when(F.col("is_buyer_maker") == False, 1).otherwise(0)).alias("buyer_initiated"),
        F.sum(F.when(F.col("is_buyer_maker") == True, 1).otherwise(0)).alias("seller_initiated")
    )
    .withColumn("buy_sell_ratio", F.col("buyer_initiated") / (F.col("seller_initiated") + 1))
    .withColumn("price_range", F.col("high_price") - F.col("low_price"))
    .withColumn("window_start", F.col("window_5min.start"))
    .withColumn("window_end", F.col("window_5min.end"))
    .drop("window_5min")
)

(gold_vwap.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(PATHS["gold_vwap"]))

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold_vwap_5min
    USING DELTA
    LOCATION '{PATHS["gold_vwap"]}'
""")

print(f"✅ Gold VWAP: {gold_vwap.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Support / Resistance

# COMMAND ----------

w20 = Window.partitionBy("symbol").orderBy("window_start").rowsBetween(-19, 0)

gold_sr = (gold_vwap
    .withColumn("rolling_low", F.min("low_price").over(w20))
    .withColumn("rolling_high", F.max("high_price").over(w20))
    .withColumn("support_level", F.when(F.col("low_price") == F.col("rolling_low"), F.col("low_price")))
    .withColumn("resistance_level", F.when(F.col("high_price") == F.col("rolling_high"), F.col("high_price")))
    .filter(F.col("support_level").isNotNull() | F.col("resistance_level").isNotNull())
    .select("symbol", "window_start", "support_level", "resistance_level", "vwap", "total_volume")
)

(gold_sr.write.format("delta").mode("overwrite").save(PATHS["gold_sr"]))

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold_support_resistance
    USING DELTA
    LOCATION '{PATHS["gold_sr"]}'
""")

print(f"✅ Gold S/R: {gold_sr.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 AI-Ready Instruction Pairs

# COMMAND ----------

enriched = spark.read.format("delta").load(PATHS["silver_enriched"])

try:
    macro = spark.read.format("delta").load(PATHS["silver_macro"]).filter(F.col("series_id") == "DFF")
    has_macro = True
except:
    macro = spark.createDataFrame([], "date DATE, value DOUBLE")
    has_macro = False

trade_w = Window.partitionBy("symbol").orderBy("trade_timestamp")

ai_ready = (enriched
    .withColumn("future_price", F.lead("price_double", 10).over(trade_w))
    .withColumn("price_change_pct", ((F.col("future_price") - F.col("price_double")) / F.col("price_double")) * 100)
    .withColumn("direction", 
        F.when(F.col("price_change_pct") > 0.5, "UP")
         .when(F.col("price_change_pct") < -0.5, "DOWN")
         .otherwise("NEUTRAL"))
)

if has_macro:
    ai_ready = ai_ready.join(
        macro.withColumnRenamed("date", "trade_date").withColumnRenamed("value", "fed_rate"),
        on=[F.to_date("trade_timestamp") == F.col("trade_date")], how="left")
else:
    ai_ready = ai_ready.withColumn("fed_rate", F.lit(None).cast("double"))

ai_df = (ai_ready
    .withColumn("instruction", 
        F.concat(F.lit("Analyze "), F.col("symbol"), F.lit(" market data:\n"),
                 F.lit("- Current Price: $"), F.round(F.col("price_double"), 2), F.lit("\n"),
                 F.lit("- 50-Trade SMA: $"), F.round(F.col("sma_50"), 2), F.lit("\n"),
                 F.lit("- Volatility: "), F.round(F.col("volatility_50"), 4), F.lit("\n"),
                 F.lit("- Fed Rate: "), F.coalesce(F.col("fed_rate").cast("string"), F.lit("N/A")), F.lit("%\n"),
                 F.lit("Predict direction in next 10 trades.")))
    .withColumn("response",
        F.concat(F.lit("Technical Analysis:\n"),
                 F.lit("Prediction: "), F.col("direction"), 
                 F.lit(" with expected "), F.round(F.abs(F.col("price_change_pct")), 2), F.lit("% movement.")))
    .select("symbol", "trade_timestamp", "instruction", "response", "direction", "price_change_pct")
)

(ai_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(PATHS["gold_llm"]))

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold_llm_training_pairs
    USING DELTA
    LOCATION '{PATHS["gold_llm"]}'
""")

print(f"✅ Gold LLM: {ai_df.count()} pairs")
display(ai_df.select("symbol", "direction", F.substring("instruction", 1, 80)).limit(3))
