from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, from_json, count, lit, desc, row_number, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.window import Window

# 1) Create SparkSession
spark = SparkSession.builder \
    .appName("NowTrendingSongs") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2) Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "music_events") \
    .option("startingOffsets", "latest") \
    .load()

# 3) Parse JSON 'value' from Kafka
schema = StructType([
    StructField("song_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),  # UNIX timestamp
    StructField("region", StringType(), True),
    StructField("action", StringType(), True)
])

json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
parsed_df = json_df.select(from_json(col("json_str"), schema).alias("data"))
events_df = parsed_df.select("data.*")

# 4) Convert UNIX timestamp to Spark TimestampType
events_df = events_df.withColumn("event_time", from_unixtime(col("timestamp")).cast(TimestampType()))

# 5) Compute skip ratio for unpopular songs
from pyspark.sql.functions import when, sum

agg_df = events_df.groupBy("song_id").agg(
    count(col("action")).alias("total_actions"),
    sum(when(col("action") == "skip", 1).otherwise(0)).alias("skip_count")
)

skip_ratio_df = agg_df.withColumn("skip_ratio", col("skip_count") / col("total_actions"))
unpopular_songs_df = skip_ratio_df.filter(col("skip_ratio") > 0.7)

# 6) Filter only "play" events for trending songs & use event time watermark
plays_df = events_df.filter(col("action") == "play") \
    .withWatermark("event_time", "1 minute")

# 7) Group by region + 1-minute event-time window
windowed_df = plays_df \
    .groupBy(
        window(col("event_time"), "1 minute"),  
        col("region"),
        col("song_id")
    ) \
    .count()

# 8) Use foreachBatch for rank-based top N logic per region
def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print("No data in this batch.")
        return

    w = Window.partitionBy("region", "window").orderBy(desc("count"))

    ranked_df = batch_df.withColumn("rn", row_number().over(w)) \
                        .filter(col("rn") <= 3)

    # Convert DataFrame to Kafka-compatible format (key-value pair)
    kafka_df = ranked_df.selectExpr(
        "CAST(region AS STRING) AS key",
        "to_json(struct(region, song_id, count)) AS value"
    )

    # Publish top songs to Kafka topic
    kafka_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "now_trending_results") \
        .save()

# 9) Write streams
trending_query = windowed_df \
    .writeStream \
    .outputMode("update") \
    .foreachBatch(process_batch) \
    .trigger(processingTime='15 seconds') \
    .start()

unpopular_query = unpopular_songs_df \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .trigger(processingTime='5 seconds') \
    .start()

trending_query.awaitTermination()
unpopular_query.awaitTermination()
