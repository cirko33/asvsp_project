FROM python:3.7-slim

WORKDIR /app

COPY producer.py ./
RUN pip install --no-cache-dir kafka-python pandas

ENV KAFKA_BROKER="localhost:9092"
ENV KAFKA_RECONNECT_BACKOFF_MS=100

CMD python -u ./producer.py