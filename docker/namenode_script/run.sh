#!/bin/sh
NAMENODE_PATH="/batch-data"
DATANODE_PATH="/project/raw/batch"

hdfs dfs -mkdir -p "$DATANODE_PATH/movies"
echo "Created directory $DATANODE_PATH/movies"
hdfs dfs -mkdir -p "$DATANODE_PATH/reviews"
echo "Created directory $DATANODE_PATH/reviews"

echo "Finished processing files"