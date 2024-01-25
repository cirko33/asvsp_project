start_time=$(date +%s)

# STREAMING START
# cd /queries/streaming/
# /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,\org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 01.py > /dev/null & 
# sleep 20
# /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,\org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 02.py > /dev/null &
# sleep 5
# /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,\org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 03.py > /dev/null &
# sleep 5
# /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,\org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 04.py > /dev/null &
# sleep 5
# /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,\org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 05.py > /dev/null
# sleep 5

# BATCH START
cd /queries/batch/
/spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 01.py &
sleep 5
/spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 02.py &
sleep 5
/spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 03.py &
sleep 5
/spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 04.py &
sleep 5
/spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 05.py &
sleep 5
/spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 06.py &
sleep 5
/spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 07.py &
sleep 5
/spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 08.py &
sleep 5
/spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 09.py &
sleep 5
/spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 10.py

end_time=$(date +%s)
echo Execution time was $(expr $end_time - $start_time) seconds.
echo All queries started

