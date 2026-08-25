# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Environment Setup: Create .env in S3 (Serverless Safe)

# COMMAND ----------

BUCKET = "s3://aws-s3-bucket-916491575117-eu-north-1-an"

env_content = """# Crypto Pipeline Secrets
FRED_API_KEY=your_fred_api_key_here
BINANCE_API_KEY=
BINANCE_SECRET_KEY=
HF_TOKEN=
"""

# Write using Spark DataFrame API (serverless compatible)
spark.createDataFrame([(env_content,)], ["value"]).write.mode("overwrite").text(f"{BUCKET}/config/.env")

print("✅ .env file created in S3")
print(f"Location: {BUCKET}/config/.env")
