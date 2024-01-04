from os import environ
import re
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

HDFS_NAMENODE = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
MOVIE_PATH = HDFS_NAMENODE + "/project/raw/batch/movies_ready/"
OUTPUT_PATH = HDFS_NAMENODE + "/project/transform/stream/"
REVIEW_PATH = HDFS_NAMENODE + "/project/raw/batch/reviews_ready/"

ELASTIC_SEARCH_NODE = environ.get("ELASTIC_SEARCH_NODE", "elasticsearch")
ELASTIC_SEARCH_USERNAME = environ.get("ELASTIC_SEARCH_USERNAME", "elastic")
ELASTIC_SEARCH_PASSWORD = environ.get("ELASTIC_SEARCH_PASSWORD", "password")
ELASTIC_SEARCH_PORT = environ.get("ELASTIC_SEARCH_PORT", "9200")

KAFKA_BROKER = environ.get("KAFKA_BROKER", "kafka:29092")

SCHEMA = StructType([
        StructField("id", StringType(), True),
        StructField("imdb_id", StringType(), True),
        StructField("budget", IntegerType(), True),
        StructField("genres", StringType(), True),
        StructField("original_language", StringType(), True),
        StructField("original_title", StringType(), True),
        StructField("popularity", StringType(), True),
        StructField("poster_path", StringType(), True),
        StructField("production_companies", StringType(), True),
        StructField("release_date", StringType(), True),
        StructField("revenue", StringType(), True),
        StructField("runtime", StringType(), True),
        StructField("vote_average", StringType(), True),
        StructField("vote_count", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("rating", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])

def save_data(df, ELASTIC_SEARCH_INDEX):
    df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

    df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", OUTPUT_PATH + ELASTIC_SEARCH_INDEX) \
        .option("checkpointLocation", "/tmp/") \
        .start()

    df.writeStream\
        .outputMode("append")\
        .option("checkpointLocation", "/tmp/") \
        .format('org.elasticsearch.spark.sql') \
        .option("es.net.http.auth.user", ELASTIC_SEARCH_USERNAME) \
        .option("es.net.http.auth.pass", ELASTIC_SEARCH_PASSWORD) \
        .option("mergeSchema", "true") \
        .option('es.index.auto.create', 'true') \
        .option('es.nodes', f'http://{ELASTIC_SEARCH_NODE}') \
        .option('es.port', ELASTIC_SEARCH_PORT) \
        .option('es.batch.write.retry.wait', '100s') \
        .start(ELASTIC_SEARCH_INDEX)