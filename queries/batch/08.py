#!/usr/bin/python
# Average text length of reviews for movies

# /spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 08.py _

from os import environ
from sys import argv
import re
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from helper import *

spark = SparkSession \
    .builder \
    .appName("B08") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")



ELASTIC_SEARCH_INDEX = "b08"
df = spark.read.csv(path=REVIEW_PATH, header=True, inferSchema=True)
df = df.groupBy("movie") \
    .agg(
        count("movie").alias("number_of_reviews"), 
        avg(length("review")).cast("int").alias("avg_length_of_reviews")
        ) \
    .limit(1000)
save_data(df, ELASTIC_SEARCH_INDEX)
