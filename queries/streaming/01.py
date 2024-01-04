#!/usr/bin/python3

# /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 01.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from helper import *


spark = SparkSession \
    .builder \
    .appName("S01") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

ELASTIC_SEARCH_INDEX = "s01"

dfb = spark.read.csv(MOVIE_PATH, header=True, inferSchema=True)

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "movies") \
    .load() \
    .withColumn("parsed_value", from_json(col("value").cast("string"), SCHEMA))

df = df.join(dfb, dfb['name'] == df['parsed_value.original_title'], "left outer")
df = df.select(dfb['name'].alias('name'), dfb['rating'].alias('batch_rating'), df['parsed_value.rating'].alias('stream_rating'))

save_data(df, ELASTIC_SEARCH_INDEX)

spark.streams.awaitAnyTermination()