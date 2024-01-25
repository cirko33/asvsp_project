#!/usr/bin/python3
# Rating for users

# /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 02.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.types as T
from sys import argv
from helper import *


spark = SparkSession \
    .builder \
    .config(conf = get_conf("S02")) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

ELASTIC_SEARCH_INDEX = "s02"

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "movies") \
    .load() \
    .withColumn("parsed_value", from_json(col("value").cast("string"), SCHEMA)) \
    .select(col("timestamp"), col("parsed_value.*")) \
    .withWatermark("timestamp", "1 seconds")

df = df.groupBy(window("timestamp", "5 seconds"), "user_id").agg(avg("rating").alias("avg_rating")) 

save_data(df, ELASTIC_SEARCH_INDEX)

spark.streams.awaitAnyTermination()