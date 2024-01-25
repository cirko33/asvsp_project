from os import environ
import re

from pyspark import SparkConf

HDFS_NAMENODE = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
MOVIE_PATH = HDFS_NAMENODE + "/project/raw/batch/movies/"
OUTPUT_PATH = HDFS_NAMENODE + "/project/transform/batch/"
REVIEW_PATH = HDFS_NAMENODE + "/project/raw/batch/reviews/"

ELASTIC_SEARCH_NODE = environ.get("ELASTIC_SEARCH_NODE", "elasticsearch")
ELASTIC_SEARCH_USERNAME = environ.get("ELASTIC_SEARCH_USERNAME", "elastic")
ELASTIC_SEARCH_PASSWORD = environ.get("ELASTIC_SEARCH_PASSWORD", "password")
ELASTIC_SEARCH_PORT = environ.get("ELASTIC_SEARCH_PORT", "9200")

def get_conf(name):
    return SparkConf() \
            .setAppName(name) \
            .setMaster("spark://spark-master:7077") \
            .set("spark.cores.max", "8") \
            .set("spark.executor.cores", "4")


def save_data(df, ELASTIC_SEARCH_INDEX):
    df.show(truncate=False)

    df.write.json(OUTPUT_PATH + ELASTIC_SEARCH_INDEX, mode="overwrite")
    print(f"Writing data to 'http://{ELASTIC_SEARCH_NODE}:{ELASTIC_SEARCH_PORT}/{ELASTIC_SEARCH_INDEX}'")

    df.write \
        .format("org.elasticsearch.spark.sql") \
        .mode("overwrite") \
        .option("es.net.http.auth.user", ELASTIC_SEARCH_USERNAME) \
        .option("es.net.http.auth.pass", ELASTIC_SEARCH_PASSWORD) \
        .option("mergeSchema", "true") \
        .option('es.index.auto.create', 'true') \
        .option('es.nodes', f'http://{ELASTIC_SEARCH_NODE}') \
        .option('es.port', ELASTIC_SEARCH_PORT) \
        .option('es.batch.write.retry.wait', '10s') \
        .save(ELASTIC_SEARCH_INDEX)
    
    print(f"Saved to {OUTPUT_PATH} and Elasticsearch index {ELASTIC_SEARCH_INDEX}!")

def duration_to_minutes(duration):
    hours_match = re.search(r'(\d+)h', duration)
    minutes_match = re.search(r'(\d+)min', duration)
    hours = int(hours_match.group(1)) if hours_match else 0
    minutes = int(minutes_match.group(1)) if minutes_match else 0

    return hours * 60 + minutes