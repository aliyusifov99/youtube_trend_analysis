# Databricks notebook source
# MAGIC %md
# MAGIC ### 1: Configure Access and Define Paths
# MAGIC

# COMMAND ----------

# CONFIGURATION FOR DELTA LAKE COMPATIBILITY
print("Setting Delta Lake configurations for high compatibility...")

spark.conf.set("spark.databricks.delta.properties.defaults.enableDeletionVectors", "false")
spark.conf.set("spark.databricks.delta.properties.defaults.checkpoint.v2.enabled", "false")
spark.conf.set("spark.databricks.delta.properties.defaults.columnMapping.mode", "none")

print("✅ Configurations set.")

# COMMAND ----------

# --- Imports ---
from pyspark.sql.functions import col, sum, avg, count, max, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import pyspark.sql.functions as F

# --- Configuration ---
storage_account_name = "your_storage_account_name_here"
# In a real scenario, use Databricks secrets:
# storage_account_key = dbutils.secrets.get(scope="your_scope", key="your_key")
storage_account_key = "your_storage_account_key_here"

spark.conf.set(
  f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
  storage_account_key
)

# --- Paths ---
silver_path = f"wasbs://silver@{storage_account_name}.blob.core.windows.net/trending_videos"
gold_base_path = f"wasbs://gold@{storage_account_name}.blob.core.windows.net"

# --- Read Only New Data ---
print("Reading clean data from Silver layer...")
df_silver = spark.read.format("delta").load(silver_path)

# Find the most recent ingestion date in the Silver table
latest_ingestion_date = df_silver.select(max("ingestion_date")).collect()[0][0]

# Filter the DataFrame to process only the new data from the latest ingestion
df_new_data = df_silver.filter(col("ingestion_date") == latest_ingestion_date)
# Cache the new data as it will be used multiple times
df_new_data.cache()

print(f"✅ Setup complete. Processing new data for ingestion date: {latest_ingestion_date}")
display(df_new_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2: Gold Table 1 - Daily Category Performance (For Trend Analysis)
# MAGIC

# COMMAND ----------

# --- Aggregate daily performance by category ---
print("Creating Gold Table 1: Daily Category Performance...")
df_daily_category_perf = df_new_data.groupBy("ingestion_date", "category_name") \
    .agg(
        sum("view_count").alias("total_views"),
        sum("like_count").alias("total_likes"),
        sum("comment_count").alias("total_comments"),
        count("video_id").alias("video_count")
    )

# --- Write to Gold, partitioned by date ---
gold_path_daily_category = f"{gold_base_path}/daily_category_performance"

snapshot_date = df_new_data.select("ingestion_date").distinct().collect()[0][0]
snapshot_date_str = snapshot_date.strftime('%Y-%m-%d') # We need the date as a string for the query

df_daily_category_perf.write \
    .format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", f"ingestion_date = '{snapshot_date_str}'") \
    .partitionBy("ingestion_date") \
    .save(gold_path_daily_category)

print(f"✅ Successfully wrote to {gold_path_daily_category}")
display(df_daily_category_perf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3: Gold Table 2 - Overall Channel Performance (Using MERGE)
# MAGIC

# COMMAND ----------


print("Creating Gold Table: Overall Channel Performance (Full Recalculation)...")

# Ensure you are reading the FULL silver table here, not a daily slice
df_silver_full = spark.read.format("delta").load(silver_path)

df_gold_channel_performance = df_silver_full.groupBy("channel_title") \
    .agg(
        count("video_id").alias("total_videos"),
        sum("view_count").alias("total_views"),
        sum("like_count").alias("total_likes")
    )

# --- Write to Gold using a simple overwrite ---
gold_path_channel = f"{gold_base_path}/channel_performance"

df_gold_channel_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_path_channel)

print(f"✅ Successfully wrote channel performance table to {gold_path_channel}")
display(df_gold_channel_performance)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4: Gold Table 3 - Most Liked Video Per Day
# MAGIC

# COMMAND ----------

# --- Use a Window function to find the most liked video within the new data ---
print("Creating Gold Table 3: Most Liked Video Per Day...")
window_spec = Window.partitionBy("ingestion_date").orderBy(col("like_count").desc())

df_top_videos = df_new_data.withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") == 1) \
    .select(
        "ingestion_date",
        "video_id",
        "video_title",
        "channel_title",
        "category_name",
        "view_count",
        "like_count"
    )

# --- Write to Gold ---
gold_path_top_videos = f"{gold_base_path}/daily_most_liked_video"
date_str_for_partition = latest_ingestion_date.strftime('%Y-%m-%d')

print(f"Writing top video for partition {date_str_for_partition}...")

df_top_videos.write \
    .format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", f"ingestion_date = '{date_str_for_partition}'") \
    .partitionBy("ingestion_date") \
    .save(gold_path_top_videos)

print(f"✅ Successfully wrote to {gold_path_top_videos}")
display(df_top_videos)

# Clean up the cache
df_new_data.unpersist()

# COMMAND ----------

