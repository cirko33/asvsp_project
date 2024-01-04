#!/usr/bin/python
# /spark/bin/spark-submit batch_pretransform.py

import re
from os import environ, path
from urllib.parse import unquote
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

HDFS_NAMENODE = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
MOVIE_PATH = HDFS_NAMENODE + "/project/raw/batch/movies/"
OUTPUT_PATH = HDFS_NAMENODE + "/project/transform/batch/"
REVIEW_PATH = HDFS_NAMENODE + "/project/raw/batch/reviews/"

ELASTIC_SEARCH_NODE = environ.get("ELASTIC_SEARCH_NODE", "elasticsearch")
ELASTIC_SEARCH_USERNAME = environ.get("ELASTIC_SEARCH_USERNAME", "elastic")
ELASTIC_SEARCH_PASSWORD = environ.get("ELASTIC_SEARCH_PASSWORD", "password")
ELASTIC_SEARCH_PORT = environ.get("ELASTIC_SEARCH_PORT", "9200")

spark = SparkSession.builder.appName("FilterData").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def transform_filename(relative_path):
    file = path.basename(relative_path)
    return unquote(file).replace(".csv", "")

df = spark.read.csv(path="/batch-data/movies/", header=True, inferSchema=True)
df = df.dropDuplicates(['name', 'year'])
df.write.partitionBy("year").csv(path=MOVIE_PATH, header=True, mode="overwrite")

df = spark.read.csv(path="/batch-data/reviews/", header=True, inferSchema=True).withColumn("movie", udf(transform_filename, StringType())(input_file_name()))
df.write.partitionBy("movie").csv(path=REVIEW_PATH, header=True, mode="overwrite")
