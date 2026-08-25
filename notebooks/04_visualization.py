# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Visualization & Analytics Dashboard

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## VWAP vs Price Chart

# COMMAND ----------

df_vwap = spark.table("gold_vwap_5min").toPandas()
btc_df = df_vwap[df_vwap["symbol"] == "BTCUSDT"].sort_values("window_start")

fig, ax1 = plt.subplots(figsize=(12, 6))
ax1.plot(btc_df["window_start"], btc_df["avg_price"], color="orange", linewidth=2, label="Avg Price")
ax1.plot(btc_df["window_start"], btc_df["vwap"], color="blue", linestyle="--", label="VWAP")
ax1.fill_between(btc_df["window_start"], btc_df["low_price"], btc_df["high_price"], alpha=0.2, color="gray", label="Range")
ax1.set_xlabel("Time")
ax1.set_ylabel("Price (USDT)")
ax1.legend(loc="upper left")
ax1.set_title("BTCUSDT: 5-Min VWAP Analysis")

ax2 = ax1.twinx()
ax2.bar(btc_df["window_start"], btc_df["total_volume"], alpha=0.3, color="green", width=0.003, label="Volume")
ax2.set_ylabel("Volume")
ax2.legend(loc="upper right")

plt.xticks(rotation=45)
plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Buy/Sell Pressure Gauge

# COMMAND ----------

latest = spark.table("gold_vwap_5min").groupBy("symbol").agg(
    F.avg("buy_sell_ratio").alias("avg_pressure")
).toPandas()

fig, ax = plt.subplots(figsize=(8, 4))
colors = ["red" if x < 1 else "green" for x in latest["avg_pressure"]]
bars = ax.bar(latest["symbol"], latest["avg_pressure"], color=colors)
ax.axhline(y=1, color="black", linestyle="--", label="Neutral (1.0)")
ax.set_ylabel("Buy/Sell Ratio")
ax.set_title("Current Market Pressure")
ax.legend()

for bar in bars:
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2., height, f'{height:.2f}', ha='center', va='bottom')

display(fig)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Latest 5-min stats
# MAGIC SELECT 
# MAGIC   symbol,
# MAGIC   window_start,
# MAGIC   vwap,
# MAGIC   price_volatility,
# MAGIC   total_volume,
# MAGIC   CASE 
# MAGIC     WHEN buy_sell_ratio > 1.2 THEN '🟢 Strong Buy'
# MAGIC     WHEN buy_sell_ratio > 1.0 THEN '🟩 Buy'
# MAGIC     WHEN buy_sell_ratio < 0.8 THEN '🔴 Strong Sell'
# MAGIC     ELSE '⬜ Neutral'
# MAGIC   END as signal
# MAGIC FROM gold_vwap_5min
# MAGIC ORDER BY window_start DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Support/Resistance levels
# MAGIC SELECT *
# MAGIC FROM gold_support_resistance
# MAGIC WHERE symbol = 'BTCUSDT'
# MAGIC ORDER BY window_start DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- AI Training data preview
# MAGIC SELECT symbol, direction, LEFT(instruction, 80) as instruction_preview
# MAGIC FROM gold_llm_training_pairs
# MAGIC LIMIT 5
