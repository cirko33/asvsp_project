docker exec -it namenode /script/run.sh
docker exec -it spark-master bash -c "spark/bin/spark-submit /batch-data/batch_pretransform.py"
docker exec -it spark-master bash -c "chmod +x /queries/orchestrate.sh && /queries/orchestrate.sh"
docker exec -it spark-master bash
