#!/usr/bin/python
# Average duration of movies in genres

# /spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 03.py

from os import environ
from sys import argv
import re
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from helper import *

spark = SparkSession \
    .builder \
    .appName("B03") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
ELASTIC_SEARCH_INDEX = "b03"

df = spark.read.csv(path=MOVIE_PATH, header=True, inferSchema=True)
df = df.withColumn("genre", explode(split(regexp_replace("genres", "\s+", ""), ";"))) \
    .filter("genre != ''") \
    .withColumn("time_in_minutes", udf(duration_to_minutes, IntegerType())("run_length")) \
    .groupBy("genre") \
    .agg(avg("time_in_minutes").cast("int").alias("avg_run_length_in_minutes")) 

save_data(df, ELASTIC_SEARCH_INDEX)
