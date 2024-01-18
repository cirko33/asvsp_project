#!/usr/bin/python
# Longest and shortest movies

# /spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 04.py

from os import environ
from sys import argv
import re
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from helper import *

spark = SparkSession \
    .builder \
    .appName("B04") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

ELASTIC_SEARCH_INDEX = "b04"

df = spark.read.csv(path=MOVIE_PATH, header=True, inferSchema=True)

df = df.withColumn("time_in_minutes", udf(duration_to_minutes, IntegerType())("run_length")) \

min_length = df.agg(min("time_in_minutes")).collect()[0][0]
max_length = df.agg(max("time_in_minutes")).collect()[0][0]
min_name = df.filter(col("time_in_minutes") == min_length).collect()[0][0]
max_name = df.filter(col("time_in_minutes") == max_length).collect()[0][0]

df = spark.createDataFrame([{ "min_length_movie": min_name, "min_length": min_length, "max_length_movie": max_name, "max_length": max_length}])

save_data(df, ELASTIC_SEARCH_INDEX)
