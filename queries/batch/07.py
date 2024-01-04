#!/usr/bin/python
# Average rating of movies in a specified year

# /spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 07.py _

from os import environ
from sys import argv
import re
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from helper import *

spark = SparkSession \
    .builder \
    .appName("B07") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

USERNAME = argv[1] if len(argv) > 1 else "reedcom"

ELASTIC_SEARCH_INDEX = "b07"

df = spark.read.csv(path=REVIEW_PATH, header=True, inferSchema=True)
df = df.filter(col("username") == USERNAME)

save_data(df, ELASTIC_SEARCH_INDEX)
