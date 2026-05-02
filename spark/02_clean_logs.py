from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

spark = SparkSession.builder \
    .appName("CleanLogs") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# =========================
# 1. Đọc RAW Delta
# =========================
df = spark.readStream \
    .format("delta") \
    .load("data/delta/raw_video_logs")

# =========================
# 2. CLEAN DATA
# =========================
df_clean = df \
    .withColumn("event_time", to_timestamp(col("event_time"))) \
    .filter(col("video_id").isNotNull()) \
    .filter(col("user_id").isNotNull()) \
    .filter(col("watch_time") > 0)

# =========================
# 3. Ghi CLEAN Delta
# =========================
query = df_clean.writeStream \
    .format("delta") \
    .option("path", "data/delta/clean_video_logs") \
    .option("checkpointLocation", "data/delta/checkpoints/clean_video_logs") \
    .outputMode("append") \
    .start()

query.awaitTermination()