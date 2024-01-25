#!/usr/bin/python
# Movies in a specified genre, duration and rating

# /spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 05.py [genre] [duration] [rating]

from os import environ
from sys import argv
import re
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from helper import *

spark = SparkSession \
    .builder \
    .config(conf = get_conf("B05")) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

GENRE = argv[1] if len(argv) > 1 else "_"
DURATION = argv[2] if len(argv) > 2 else "_"
RATING = argv[3] if len(argv) > 3 else "_"

ELASTIC_SEARCH_INDEX = "b05"

df = spark.read.csv(path=MOVIE_PATH, header=True, inferSchema=True)
df.withColumn("time_in_minutes", udf(duration_to_minutes, IntegerType())("run_length")) 

if GENRE != "_":
    df = df.withColumn("genre", explode(split(regexp_replace("genres", "\s+", ""), ";"))) \
        .filter("genre != ''") \
        .filter(col("genre") == GENRE)

if DURATION != "_":
    df = df.filter(col("time_in_minutes") == DURATION)

if RATING != "_":
    df = df.filter(col("rating") >= RATING)

df = df.select("name", "year", "rating", "run_length", "genres")

save_data(df, ELASTIC_SEARCH_INDEX)
