#!/usr/bin/python3
# Top 5 movies with the highest earning percentage

# /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 05.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.types as T
from sys import argv
from helper import *


spark = SparkSession \
    .builder \
    .appName("S05") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

ELASTIC_SEARCH_INDEX = "s05"

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "movies") \
    .load() \
    .withColumn("parsed_value", from_json(col("value").cast("string"), SCHEMA)) \
    .select(col("timestamp"), col("parsed_value.*"))

df = df.groupBy("original_title").agg(avg("budget").alias("budget"), avg("revenue").alias("revenue")) \
    .withColumn("earning_percentage", col("revenue") / col("budget") * 100)
df = df.orderBy(col("earning_percentage").desc())
df = df.select(col("original_title").alias('title'), col("budget").cast(IntegerType()), col("revenue").cast(IntegerType()), col("earning_percentage").cast(IntegerType())).limit(5)

save_data(df, ELASTIC_SEARCH_INDEX, True)

spark.streams.awaitAnyTermination()