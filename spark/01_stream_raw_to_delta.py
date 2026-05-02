from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

# =============================
# 1. Spark Session + Delta config
# =============================
spark = SparkSession.builder \
    .appName("KafkaToDelta") \
    .config("spark.sql.shuffle.partitions", "3") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =============================
# 2. Schema (giống người 1)
# =============================
schema = StructType([
    StructField("event_time", StringType()),
    StructField("user_id", IntegerType()),
    StructField("video_id", IntegerType()),
    StructField("watch_time", DoubleType()),
    StructField("duration", DoubleType()),
    StructField("watch_ratio", DoubleType()),
    StructField("is_click", IntegerType()),
    StructField("is_like", IntegerType()),
    StructField("is_share", IntegerType()),
])

# =============================
# 3. Đọc Kafka
# =============================
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "video_logs") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# =============================
# 4. Parse JSON
# =============================
df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# =============================
# 5. Ghi Delta (RAW LAYER)
# =============================
query = df_parsed.writeStream \
    .format("delta") \
    .option("path", "data/delta/raw_video_logs") \
    .option("checkpointLocation", "data/delta/checkpoints/raw_video_logs") \
    .outputMode("append") \
    .start()

query.awaitTermination()