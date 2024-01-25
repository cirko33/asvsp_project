#!/usr/bin/python
# Average duration of movies in genres

# /spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 03.py

from os import environ
from sys import argv
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from helper import *

spark = SparkSession \
    .builder \
    .config(conf = get_conf("B03")) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
ELASTIC_SEARCH_INDEX = "b03"

def duration_to_minutes(duration):
    hours_match = re.search(r'(\d+)h', duration)
    minutes_match = re.search(r'(\d+)min', duration)
    hours = int(hours_match.group(1)) if hours_match else 0
    minutes = int(minutes_match.group(1)) if minutes_match else 0

    return hours * 60 + minutes

df = spark.read.csv(path=MOVIE_PATH, header=True, inferSchema=True)
df = df.withColumn("genre", explode(split(regexp_replace("genres", "\s+", ""), ";"))) \
    .filter("genre != ''") \
    .withColumn("time_in_minutes", udf(duration_to_minutes, IntegerType())("run_length")) \
    .groupBy("genre") \
    .agg(avg("time_in_minutes").cast("int").alias("avg_run_length_in_minutes")) 

save_data(df, ELASTIC_SEARCH_INDEX)
