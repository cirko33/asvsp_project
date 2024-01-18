#!/usr/bin/python3
# Average rating for categories

# /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 04.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.types as T
from sys import argv
from helper import *


spark = SparkSession \
    .builder \
    .appName("S04") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

ELASTIC_SEARCH_INDEX = "s04"

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "movies") \
    .load() \
    .withColumn("parsed_value", from_json(col("value").cast("string"), SCHEMA)) \
    .select(col("timestamp"), col("parsed_value.*"))

df = df.withColumn("genres", from_json(col("genres"), T.ArrayType(T.MapType(T.StringType(), T.StringType()))))
df = df.select(col("*"), explode("genres").alias("genre"))
df = df.groupBy("genre.name").agg(avg("rating").alias("avg_rating"))
save_data(df, ELASTIC_SEARCH_INDEX, True)

spark.streams.awaitAnyTermination()