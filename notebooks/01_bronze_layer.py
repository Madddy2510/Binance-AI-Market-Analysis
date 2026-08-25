# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Layer: Binance + FRED → S3 (Self-Contained)

# COMMAND ----------

import requests
import json
import time
import os
from datetime import datetime
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load .env from S3

# COMMAND ----------

BUCKET = "s3://aws-s3-bucket-916491575117-eu-north-1-an"

lines = [row.value for row in spark.read.text(f"{BUCKET}/config/.env").collect()]
for line in lines:
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    key, value = line.split("=", 1)
    os.environ[key.strip()] = value.strip()

def get_env(key, default=None):
    return os.environ.get(key, default)

print(f"✅ .env loaded. FRED key present: {'Yes' if get_env('FRED_API_KEY') else 'No'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hardcoded Paths (No Config File Needed)

# COMMAND ----------

BUCKET = "s3://aws-s3-bucket-916491575117-eu-north-1-an"

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

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

FRED_SERIES = {
    "DFF": "fed_funds_rate",
    "T10Y2Y": "yield_spread_10y_2y",
    "VIXCLS": "vix_index"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Ingest Binance Trades

# COMMAND ----------

def fetch_binance(symbol):
    url = f"https://api.binance.com/api/v3/trades?symbol={symbol}&limit=1000"
    try:
        resp = requests.get(url, timeout=15)
        count = len(resp.json()) if resp.status_code == 200 else 0
        print(f"{symbol}: HTTP {resp.status_code}, trades={count}")
        return resp.json() if resp.status_code == 200 else []
    except Exception as e:
        print(f"{symbol} ERROR: {e}")
        return []

all_trades = []
for sym in SYMBOLS:
    trades = fetch_binance(sym)
    for t in trades:
        t["symbol"] = sym
        t["ingestion_time"] = datetime.utcnow().isoformat()
        t["source"] = "binance_rest"
        t["trade_date"] = datetime.utcnow().strftime("%Y-%m-%d")
    all_trades.extend(trades)
    time.sleep(0.3)

print(f"\nTotal trades fetched: {len(all_trades)}")

if all_trades:
    df = spark.createDataFrame(all_trades)
    df = (df
        .withColumn("id", F.col("id").cast("long"))
        .withColumn("price", F.col("price").cast("decimal(18,8)"))
        .withColumn("qty", F.col("qty").cast("decimal(18,8)"))
        .withColumn("quoteQty", F.col("quoteQty").cast("decimal(18,8)"))
        .withColumn("time", F.col("time").cast("long"))
        .withColumn("isBuyerMaker", F.col("isBuyerMaker").cast("boolean"))
        .withColumn("isBestMatch", F.col("isBestMatch").cast("boolean"))
    )

    (df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(PATHS["bronze_binance"]))

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS bronze_binance_trades
        USING DELTA
        LOCATION '{PATHS["bronze_binance"]}'
    """)

    print(f"✅ Bronze Binance: {df.count()} trades written")
    display(df.limit(5))
else:
    print("❌ No trades fetched")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Ingest FRED Macro Data

# COMMAND ----------

FRED_API_KEY = get_env("FRED_API_KEY")
all_macro = []

if not FRED_API_KEY:
    print("❌ FRED_API_KEY missing in .env")
else:
    print(f"FRED key: {FRED_API_KEY[:8]}...")

    for series_id in FRED_SERIES.keys():
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": series_id,
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "observation_start": "2020-01-01",
            "sort_order": "desc",
            "limit": 5000
        }
        try:
            resp = requests.get(url, params=params, timeout=20)
            data = resp.json()
            print(f"{series_id}: HTTP {resp.status_code}")

            if "observations" in data:
                for obs in data["observations"]:
                    all_macro.append({
                        "date": obs["date"],
                        "series_id": series_id,
                        "indicator_name": FRED_SERIES[series_id],
                        "value": float(obs["value"]) if obs["value"] != "." else None,
                        "ingestion_time": datetime.utcnow().isoformat()
                    })
            elif "error_code" in data:
                print(f"  FRED error: {data}")
        except Exception as e:
            print(f"Error {series_id}: {e}")
        time.sleep(0.3)

if all_macro:
    df_macro = spark.createDataFrame(all_macro)
    (df_macro.write
        .format("delta")
        .mode("overwrite")
        .save(PATHS["bronze_fred"]))

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS bronze_fred_macro
        USING DELTA
        LOCATION '{PATHS["bronze_fred"]}'
    """)

    print(f"✅ Bronze FRED: {len(all_macro)} records")
    display(df_macro.limit(5))
else:
    print("⚠️ No FRED data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Bronze Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'binance' as source, COUNT(*) as cnt FROM bronze_binance_trades
# MAGIC UNION ALL
# MAGIC SELECT 'fred', COUNT(*) FROM bronze_fred_macro
