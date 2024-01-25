#!/usr/bin/python
# Average rating of movies in genres

# /spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 02.py

from os import environ
from sys import argv
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from helper import *

spark = SparkSession \
    .builder \
    .config(conf = get_conf("B02")) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
ELASTIC_SEARCH_INDEX = "b02"

df = spark.read.csv(path=MOVIE_PATH, header=True, inferSchema=True)
df = df.withColumn("genre", explode(split(regexp_replace("genres", "\s+", ""), ";"))) \
    .filter("genre != ''") \
    .groupBy("genre").agg(round(avg("rating"), 1).alias("avg_rating"))

save_data(df, ELASTIC_SEARCH_INDEX)
