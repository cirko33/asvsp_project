#!/usr/bin/python
# Review statistics per genre

# /spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 10.py

from os import environ
from sys import argv
import re
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from helper import *

spark = SparkSession \
    .builder \
    .config(conf = get_conf("B10")) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

ELASTIC_SEARCH_INDEX = "b10"

df = spark.read.csv(path=REVIEW_PATH, header=True, inferSchema=True)
df = df.groupBy("movie") \
    .agg(
        format_number(avg("rating"), 1).alias("avg_rating"),
        count("rating").alias("num_raters_revs")
        )

dfb = spark.read.csv(path=MOVIE_PATH, header=True, inferSchema=True)
dfb = dfb.withColumn("name", regexp_replace(col("name"), ":", "-"))

df = df.join(dfb, df.movie == concat(dfb.name, lit(" "), dfb.year), "inner")
df = df.withColumn("genre", explode(split(regexp_replace("genres", "\s+", ""), ";"))) \
    .filter("genre != ''") \
    .groupBy("genre") \
    .agg(
        round(avg("avg_rating"),1).cast("float").alias("avg_rating"),
        sum("num_raters_revs").alias("num_raters"),
        ) \
    .limit(100)

save_data(df, ELASTIC_SEARCH_INDEX)

