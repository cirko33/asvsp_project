#!/usr/bin/python
# Median rating of movies

# /spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 09.py _

from os import environ
from sys import argv
import re
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from helper import *

spark = SparkSession \
    .builder \
    .appName("B09") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

ELASTIC_SEARCH_INDEX = "b09"

df = spark.read.csv(path=REVIEW_PATH, header=True, inferSchema=True)
df = df.groupBy("movie") \
    .agg(
        format_number(avg("rating"), 1).alias("avg_rating"),
        count("rating").alias("num_raters_revs")
        )

dfb = spark.read.csv(path=MOVIE_PATH, header=True, inferSchema=True)
dfb = dfb.withColumn("name", regexp_replace(col("name"), ":", "-"))

df = df.join(dfb, df.movie == concat(dfb.name, lit(" "), dfb.year), "inner") \
    .select("name", "year", "avg_rating", dfb.rating, "num_raters", "num_raters_revs") \
    .limit(100)

save_data(df, ELASTIC_SEARCH_INDEX)

