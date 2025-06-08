# Databricks notebook source
# MAGIC %md
# MAGIC ### 1: Configure Access to Azure Storage

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, current_timestamp, lit, explode

# --- Configuration ---
# In a real scenario, use Databricks secrets:
# storage_account_key = dbutils.secrets.get(scope="your_scope", key="your_key")
storage_account_name = "your_storage_account_name_here"
storage_account_key = "your_storage_account_key_here"
silver_container_name = "silver"

spark.conf.set(
  f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
  storage_account_key
)
print("✅ Spark configuration set.")


# --- Dynamically find the latest date partition ---
print("Scanning for the latest date partition in the bronze container...")
all_folders = dbutils.fs.ls(f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/")

# Filter for directories that look like dates, e.g., "2025-06-08/"
date_folders = [f.name.strip('/') for f in all_folders if f.isDir() and f.name[:-1].replace('-', '').isdigit()]

if not date_folders:
  raise Exception("No date folders found in the bronze container.")

# Find the most recent date string
latest_date_str = max(date_folders)
print(f"Found latest folder: {latest_date_str}")

# --- Widget for Manual Overrides ---
# The widget now defaults to the latest folder found.
# Its main purpose is to allow manual reprocessing of a different date.
dbutils.widgets.text("processing_date", latest_date_str, "Processing Date (YYYY-MM-DD)")
processing_date_str = dbutils.widgets.get("processing_date")

print(f"✅ Processing for date: {processing_date_str}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2: Define the Path and Read the Bronze Data
# MAGIC

# COMMAND ----------

bronze_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/{processing_date_str}/trending_videos_*.json"

category_ref_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/PL_category_id.json"

print(f"Reading raw data from: {bronze_path}")
df_bronze = spark.read.option("multiLine", "true").json(bronze_path)

print(f"Reading category reference data from: {category_ref_path}")
df_categories = spark.read.option("multiLine", "true").json(category_ref_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3: Clean and Transform the Data
# MAGIC

# COMMAND ----------

# Flatten category data
df_categories_flat = df_categories.select(explode("items").alias("items")) \
    .select(col("items.id").alias("category_id"), col("items.snippet.title").alias("category_name"))

# Flatten, clean, and cast the main dataset
df_silver_base = df_bronze.select(
    col("id").alias("video_id"),
    col("snippet.title").alias("video_title"),
    to_timestamp(col("snippet.publishedAt")).alias("publish_timestamp"),
    col("snippet.channelTitle").alias("channel_title"),
    col("snippet.categoryId").alias("category_id"),
    col("snippet.tags").alias("tags"),
    col("statistics.viewCount").cast("long").alias("view_count"),
    col("statistics.likeCount").cast("long").alias("like_count"),
    col("statistics.commentCount").cast("long").alias("comment_count")
)

# Add metadata and join with category names
df_silver_final = df_silver_base \
    .join(df_categories_flat, "category_id", "left") \
    .withColumn("processed_timestamp", current_timestamp()) \
    .withColumn("ingestion_date", lit(processing_date_str).cast("date"))

print("✅ Transformation and enrichment complete.")
display(df_silver_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4: Write the Cleaned Data to the Silver Layer
# MAGIC

# COMMAND ----------

silver_path = f"wasbs://{silver_container_name}@{storage_account_name}.blob.core.windows.net/trending_videos"

print(f"Writing data for partition {processing_date_str} to Silver table...")

df_silver_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", f"ingestion_date = '{processing_date_str}'") \
    .partitionBy("ingestion_date") \
    .save(silver_path)

print("✅ Write complete.")

# COMMAND ----------

