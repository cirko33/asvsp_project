#!/usr/bin/python3
# Difference between stream rating and average rating?

# /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,\org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 01.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from helper import *

spark = SparkSession \
    .builder \
    .config(conf = get_conf("S01")) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

ELASTIC_SEARCH_INDEX = "s01"
dfb = spark.read.csv(MOVIE_PATH, header=True, inferSchema=True)

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "movies") \
    .load() \
    .withColumn("parsed_value", from_json(col("value").cast("string"), SCHEMA)) \
    .select(col("parsed_value.*")) \
    .withColumn("stream_rating", col("rating"))

df = df.join(dfb, dfb['name'] == df['original_title'], "left")
df = df.withColumn("different", when(abs(dfb.rating - df.stream_rating) > 2, "YES").otherwise("NO"))
df = df.select(dfb['name'].alias('name'), dfb['rating'].alias('batch_rating'), df['stream_rating'].alias('stream_rating'), df['different'].alias('different'))

save_data(df, ELASTIC_SEARCH_INDEX)

spark.streams.awaitAnyTermination()