#!/usr/bin/python
# Reviews of movie Y that have rating >= X

# /spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 06.py [rating] [movie]

from os import environ
from sys import argv
import re
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from helper import *

spark = SparkSession \
    .builder \
    .appName("B06") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
def get_name(list):
    name = ""
    for i in list:
        name += i + "X"
    
    return name[:-1].replace(":","#")

RATING = argv[1] if len(argv) > 1 else "5"
MOVIE = get_name([arg for arg in argv[2:]]) if len(argv) > 2 else "The Dark Knight 2008"

ELASTIC_SEARCH_INDEX = "b06"

df = spark.read.csv(path=REVIEW_PATH, header=True, inferSchema=True)
df = df.filter(col("rating") >= RATING and col("movie") == MOVIE)

save_data(df, ELASTIC_SEARCH_INDEX)

