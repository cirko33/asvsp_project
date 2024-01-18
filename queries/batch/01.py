#!/usr/bin/python
# Movies in a specified year and their rating

# /spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 01.py [year]

from sys import argv
from pyspark.sql import SparkSession
from helper import *

spark = SparkSession \
    .builder \
    .appName("B01") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

YEAR = argv[1] if len(argv) > 1 else "2010"
ELASTIC_SEARCH_INDEX = "b01"

df = spark.read.csv(path=MOVIE_PATH, header=True, inferSchema=True)
df = df.filter(df['year'] == YEAR)

save_data(df, ELASTIC_SEARCH_INDEX)
