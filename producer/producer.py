from os import environ
from json import dumps
from kafka import KafkaProducer
import kafka.errors
import pandas as pd
import time

KAFKA_CONFIGURATION = {
    "bootstrap_servers": environ.get("KAFKA_BROKER", "localhost:29092").split(","),
    "key_serializer": lambda x: str.encode("" if not x else x, encoding='utf-8'),
    "value_serializer": lambda x: dumps(dict() if not x else x).encode(encoding='utf-8'),
    "reconnect_backoff_ms": int(environ.get("KAFKA_RECONNECT_BACKOFF_MS", "100"))
}

def get_connection():
    while True:
        try:
            producer = KafkaProducer(**KAFKA_CONFIGURATION)
            print("Connected to Kafka!")
            return producer
        except kafka.errors.NoBrokersAvailable as e:
            print(e)
            time.sleep(3)

def producing():
    producer = get_connection()
    movies = pd.read_csv("./data/movies.csv").to_dict('records')
    movies = {int(movie["id"]): movie for movie in movies}
    with open("./data/ratings.csv", "r") as f:
        for line in f:
            rating = line.strip().split(",")
            movie_id = rating[1]
            movie = movies.get(int(movie_id))
            if not movie:
                continue
      
            movie["user_id"] = rating[0]
            movie["rating"] = rating[2]
            # movie["timestamp"] = rating[3]
            print(str(movie))
            producer.send("movies", key=movie_id + "_" + movie["user_id"], value=movie)
            time.sleep(5)

if __name__ == "__main__":
    producing()